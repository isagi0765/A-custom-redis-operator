/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	cachev1 "yourorg.io/redis-cluster-operator/api/v1"
)

// RedisClusterReconciler reconciles a RedisCluster object
type RedisClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=cache.yourorg.io,resources=redisclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cache.yourorg.io,resources=redisclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cache.yourorg.io,resources=redisclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;delete

func labelsFor(name string) map[string]string {
	return map[string]string{
		"app":                                 "redis-cluster",
		"redis-cluster.cache.yourorg.io/name": name,
	}
}

func nodeInclusionPolicyPtr(p corev1.NodeInclusionPolicy) *corev1.NodeInclusionPolicy {
	return &p
}

// desiredHeadlessService builds the governing headless Service every
// StatefulSet requires for stable pod network identity.
func desiredHeadlessService(rc *cachev1.RedisCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rc.Name + "-headless",
			Namespace: rc.Namespace,
			Labels:    labelsFor(rc.Name),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  labelsFor(rc.Name),
			Ports: []corev1.ServicePort{
				{Name: "redis", Port: 6379, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt32(6379)},
				{Name: "gossip", Port: 16379, Protocol: corev1.ProtocolTCP, TargetPort: intstr.FromInt32(16379)},
			},
		},
	}
}

func syncHeadlessService(existing, desired *corev1.Service) bool {
	changed := !reflect.DeepEqual(existing.Labels, desired.Labels) ||
		!reflect.DeepEqual(existing.Spec.Selector, desired.Spec.Selector) ||
		!reflect.DeepEqual(existing.Spec.Ports, desired.Spec.Ports)
	if changed {
		existing.Labels = desired.Labels
		existing.Spec.Selector = desired.Spec.Selector
		existing.Spec.Ports = desired.Spec.Ports
	}
	return changed
}

func statefulSetUnsupportedChange(existing, desired *appsv1.StatefulSet) string {
	existingReplicas := int32(1)
	if existing.Spec.Replicas != nil {
		existingReplicas = *existing.Spec.Replicas
	}
	if desired.Spec.Replicas != nil && existingReplicas != *desired.Spec.Replicas {
		return "changing node or replica counts requires Redis Cluster resharding"
	}
	if existing.Spec.ServiceName != desired.Spec.ServiceName ||
		!reflect.DeepEqual(existing.Spec.Selector, desired.Spec.Selector) ||
		storageTemplatesDiffer(existing.Spec.VolumeClaimTemplates, desired.Spec.VolumeClaimTemplates) {
		return "changing the StatefulSet service, selector, or storage template is not supported in place"
	}
	return ""
}

func storageTemplatesDiffer(existing, desired []corev1.PersistentVolumeClaim) bool {
	if len(existing) != len(desired) {
		return true
	}
	for i := range existing {
		oldClaim, newClaim := existing[i], desired[i]
		if oldClaim.Name != newClaim.Name ||
			!reflect.DeepEqual(oldClaim.Spec.AccessModes, newClaim.Spec.AccessModes) ||
			storageClassName(oldClaim.Spec.StorageClassName) != storageClassName(newClaim.Spec.StorageClassName) {
			return true
		}
		oldStorage, oldHasStorage := oldClaim.Spec.Resources.Requests[corev1.ResourceStorage]
		newStorage, newHasStorage := newClaim.Spec.Resources.Requests[corev1.ResourceStorage]
		if oldHasStorage != newHasStorage || (oldHasStorage && oldStorage.Cmp(newStorage) != 0) {
			return true
		}
	}
	return false
}

func storageClassName(name *string) string {
	if name == nil {
		return ""
	}
	return *name
}

func syncStatefulSetTemplate(existing, desired *appsv1.StatefulSet) bool {
	changed := !reflect.DeepEqual(existing.Labels, desired.Labels) ||
		!reflect.DeepEqual(existing.Spec.Template, desired.Spec.Template)
	if changed {
		existing.Labels = desired.Labels
		existing.Spec.Template = desired.Spec.Template
	}
	return changed
}

// desiredStatefulSet builds the StatefulSet running all nodes as uniform
// pods -- no static leader/follower identity. Role assignment happens
// later, in our own Go code, once we can see actual pod placement --
// deliberately NOT encoded as a Kubernetes scheduling constraint, which is
// exactly what caused every deadlock we hit with the pre-built operator.
func desiredStatefulSet(rc *cachev1.RedisCluster) (*appsv1.StatefulSet, error) {
	totalReplicas := rc.Spec.Nodes * (1 + rc.Spec.ReplicasPerNode)

	storageQty, err := resource.ParseQuantity(rc.Spec.StorageSize)
	if err != nil {
		return nil, fmt.Errorf("invalid storageSize %q: %w", rc.Spec.StorageSize, err)
	}

	labels := labelsFor(rc.Name)

	pvcTemplate := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: storageQty},
			},
		},
	}
	if rc.Spec.StorageClassName != "" {
		pvcTemplate.Spec.StorageClassName = &rc.Spec.StorageClassName
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rc.Name,
			Namespace: rc.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: rc.Name + "-headless",
			Replicas:    &totalReplicas,
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					// Plain, single spread rule -- no competing per-pod
					// exclusion, so HARD is safe here (unlike the
					// pre-built operator's webhook + balance-rule
					// combination, which deadlocked repeatedly because
					// two DIFFERENT hard rules fought over the same
					// pods). 6 pods / 3 nodes divides evenly, so this
					// will always be satisfiable.
					TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
						{
							MaxSkew:           1,
							TopologyKey:       "kubernetes.io/hostname",
							WhenUnsatisfiable: corev1.DoNotSchedule,
							LabelSelector:     &metav1.LabelSelector{MatchLabels: labels},
							// Without this, a tainted/unschedulable node
							// (e.g. the control-plane) still counts as a
							// valid "empty" domain in the skew math,
							// dragging the effective minimum to 0 and
							// blocking any pod past the first N-1 nodes.
							// Confirmed hands-on: the 4th pod got stuck
							// Pending with "didn't match pod topology
							// spread constraints" on all 3 real worker
							// nodes, even though 2-per-node should have
							// been well within maxSkew:1.
							NodeTaintsPolicy: nodeInclusionPolicyPtr(corev1.NodeInclusionPolicyHonor),
						},
					},
					Containers: []corev1.Container{
						{
							Name:            "redis",
							Image:           rc.Spec.Image,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Env: []corev1.EnvVar{
								{
									Name: "POD_IP",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"},
									},
								},
							},
							Command: []string{
								"redis-server",
								"--port", "6379",
								"--cluster-enabled", "yes",
								"--cluster-config-file", "/data/nodes.conf",
								"--cluster-node-timeout", "5000",
								"--appendonly", "yes",
								"--protected-mode", "no",
								"--bind", "0.0.0.0",
								// baked in from day one -- this exact
								// omission caused the stale-gossip
								// "?:6379" bug we hit twice with the
								// hand-built cluster.
								"--cluster-announce-ip", "$(POD_IP)",
							},
							Ports: []corev1.ContainerPort{
								{Name: "redis", ContainerPort: 6379},
								{Name: "gossip", ContainerPort: 16379},
							},
							Resources: rc.Spec.Resources,
							VolumeMounts: []corev1.VolumeMount{
								{Name: "data", MountPath: "/data"},
							},
						},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{pvcTemplate},
		},
	}, nil
}

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *RedisClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var redisCluster cachev1.RedisCluster
	if err := r.Get(ctx, req.NamespacedName, &redisCluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.Info("reconciling RedisCluster",
		"name", redisCluster.Name,
		"nodes", redisCluster.Spec.Nodes,
		"replicasPerNode", redisCluster.Spec.ReplicasPerNode,
	)

	// 1. Headless Service -- create if missing. Not updating existing ones
	// yet; that comes later once this basic create path is proven.
	svc := desiredHeadlessService(&redisCluster)
	if err := ctrl.SetControllerReference(&redisCluster, svc, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	var existingSvc corev1.Service
	if err := r.Get(ctx, client.ObjectKeyFromObject(svc), &existingSvc); apierrors.IsNotFound(err) {
		log.Info("creating headless service", "name", svc.Name)
		if err := r.Create(ctx, svc); err != nil {
			return ctrl.Result{}, fmt.Errorf("creating headless service: %w", err)
		}
		existingSvc = *svc
	} else if err != nil {
		return ctrl.Result{}, err
	}
	if syncHeadlessService(&existingSvc, svc) {
		if err := r.Update(ctx, &existingSvc); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating headless service: %w", err)
		}
	}

	// 2. StatefulSet -- create if missing, reconcile safe pod-template drift.
	sts, err := desiredStatefulSet(&redisCluster)
	if err != nil {
		if statusErr := r.setPhase(ctx, &redisCluster, "Failed", 0, 0); statusErr != nil {
			return ctrl.Result{}, errors.Join(err, statusErr)
		}
		return ctrl.Result{}, err
	}
	if err := ctrl.SetControllerReference(&redisCluster, sts, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	var existingSts appsv1.StatefulSet
	if err := r.Get(ctx, client.ObjectKeyFromObject(sts), &existingSts); apierrors.IsNotFound(err) {
		log.Info("creating statefulset", "name", sts.Name, "replicas", *sts.Spec.Replicas)
		if err := r.Create(ctx, sts); err != nil {
			return ctrl.Result{}, fmt.Errorf("creating statefulset: %w", err)
		}
		existingSts = *sts
	} else if err != nil {
		return ctrl.Result{}, err
	}
	if reason := statefulSetUnsupportedChange(&existingSts, sts); reason != "" {
		log.Info("RedisCluster change requires an unsupported migration",
			"reason", reason,
			"currentReplicas", existingSts.Spec.Replicas,
			"desiredReplicas", sts.Spec.Replicas,
		)
		if err := r.setPhase(ctx, &redisCluster, "UnsupportedChange",
			redisCluster.Status.ReadyMasters, redisCluster.Status.ReadyReplicas); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if syncStatefulSetTemplate(&existingSts, sts) {
		if err := r.Update(ctx, &existingSts); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating statefulset pod template: %w", err)
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, r.setPhase(ctx, &redisCluster, "Provisioning", 0, 0)
	}

	// 3. Wait for the current StatefulSet generation and all updated pods
	// before issuing Redis commands.
	if existingSts.Status.ObservedGeneration < existingSts.Generation ||
		existingSts.Status.UpdatedReplicas != *sts.Spec.Replicas ||
		existingSts.Status.ReadyReplicas != *sts.Spec.Replicas {
		log.Info("Waiting for all pods to be ready",
			"ready", existingSts.Status.ReadyReplicas,
			"updated", existingSts.Status.UpdatedReplicas,
			"observedGeneration", existingSts.Status.ObservedGeneration,
			"generation", existingSts.Generation,
			"desired", *sts.Spec.Replicas)

		var pendingPods corev1.PodList
		if err := r.List(ctx, &pendingPods,
			client.InNamespace(redisCluster.Namespace),
			client.MatchingLabels(labelsFor(redisCluster.Name)),
		); err != nil {
			return ctrl.Result{}, fmt.Errorf("listing pods while provisioning: %w", err)
		}
		if unsched, msg := podsUnschedulable(pendingPods.Items); unsched {
			log.Info("Pods are unschedulable", "detail", msg)
			if err := r.setPhaseDetail(ctx, &redisCluster, "Failed", 0, 0,
				"Unschedulable", msg); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{}, r.setPhase(ctx, &redisCluster, "Provisioning", 0, 0)
	}

	// 4. Gather the real pod -> Kubernetes node mapping. This is the input
	// that makes safe pairing possible, and it can only come from the
	// Kubernetes API -- Redis has no idea what a node is.
	var podList corev1.PodList
	if err := r.List(ctx, &podList,
		client.InNamespace(redisCluster.Namespace),
		client.MatchingLabels(labelsFor(redisCluster.Name)),
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing pods: %w", err)
	}

	if unsched, msg := podsUnschedulable(podList.Items); unsched {
		log.Info("Pods are unschedulable", "detail", msg)
		if err := r.setPhaseDetail(ctx, &redisCluster, "Failed", 0, 0,
			"Unschedulable", msg); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	nodes := make([]nodeInfo, 0, len(podList.Items))
	for _, pod := range podList.Items {
		if pod.Status.PodIP == "" || pod.Spec.NodeName == "" {
			// Still settling; come back shortly rather than acting on a
			// half-known topology.
			log.Info("Pod not fully scheduled yet, requeueing", "pod", pod.Name)
			return ctrl.Result{RequeueAfter: 5 * time.Second},
				r.setPhase(ctx, &redisCluster, "Provisioning", 0, 0)
		}
		nodes = append(nodes, nodeInfo{
			PodName: pod.Name,
			K8sNode: pod.Spec.NodeName,
			IP:      pod.Status.PodIP,
		})
	}
	// Deterministic order so repeated reconciles behave identically.
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].PodName < nodes[j].PodName })

	// Topology must be placeable before we touch Redis: one master per
	// distinct Kubernetes node.
	if _, _, err := planRoles(nodes, int(redisCluster.Spec.Nodes)); err != nil {
		log.Info("RedisCluster topology is not placeable", "error", err)
		if statusErr := r.setPhaseDetail(ctx, &redisCluster, "Failed", 0, 0,
			"InsufficientNodes", err.Error()); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// 5. Bootstrap or resume a partial bootstrap. Idempotent helpers skip
	// slots/replicas that are already correct so a mid-flight failure can
	// recover. A fully formed cluster (all slots + linked replicas) is left
	// alone even when gossip is temporarily inconsistent.
	needsWork, err := needsBootstrapOrRepair(ctx, nodes, int(redisCluster.Spec.Nodes))
	if err != nil {
		return ctrl.Result{}, err
	}
	if needsWork {
		log.Info("Bootstrapping Redis cluster", "pods", len(nodes), "masters", redisCluster.Spec.Nodes)
		if err := bootstrapOrRepair(ctx, nodes, int(redisCluster.Spec.Nodes)); err != nil {
			log.Error(err, "Bootstrap failed, will retry")
			_ = r.setPhase(ctx, &redisCluster, "Bootstrapping", 0, 0)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		log.Info("Bootstrap complete")
		// Give gossip a moment to settle before reporting roles.
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// 6. Cluster is formed. Cross-check EVERY pod's own view before
	// trusting anything -- this replaces the old "stop at the first pod
	// that answers without erroring" logic, which was the real bug: a
	// pod could answer successfully while its own gossip was still
	// incomplete after a restart, and the operator trusted it anyway,
	// once reporting "healthy" with a whole master missing.
	assessment := assessHealth(ctx, nodes, len(nodes))
	if !assessment.Complete {
		log.Info("Cluster view not yet consistent across all pods, will retry",
			"queriedPod", assessment.QueriedPod, "issues", assessment.Issues)
		if healed, healErr := healClusterMembership(ctx, nodes); healErr != nil {
			log.Error(healErr, "Cluster membership heal failed")
		} else if healed {
			log.Info("Healed cluster membership (FORGET/MEET)")
			_ = r.setPhase(ctx, &redisCluster, "Degraded", 0, 0)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		_ = r.setPhase(ctx, &redisCluster, "Degraded", 0, 0)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	byIP := map[string]*nodeInfo{}
	for i := range nodes {
		byIP[nodes[i].IP] = &nodes[i]
	}
	parseClusterNodesText(assessment.RawNodes, byIP)

	mastersReady, replicasReady := countRoles(nodes)
	if !rolesMatchSpec(nodes, redisCluster.Spec.Nodes, redisCluster.Spec.ReplicasPerNode) {
		msg := fmt.Sprintf("role counts do not match spec: masters=%d want=%d replicas=%d want=%d",
			mastersReady, redisCluster.Spec.Nodes,
			replicasReady, redisCluster.Spec.Nodes*redisCluster.Spec.ReplicasPerNode)
		log.Info("Cluster roles do not match Spec", "detail", msg)
		if err := r.setPhaseDetail(ctx, &redisCluster, "Degraded", mastersReady, replicasReady,
			"RoleMismatch", msg); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Only ever act on a view we've confirmed every pod agrees with --
	// rebalanceMasters must never fire against a partial/stale view,
	// which could misdiagnose a real imbalance or miss one entirely.
	if acted, rbErr := rebalanceMasters(ctx, nodes); rbErr != nil {
		log.Error(rbErr, "Master rebalance check failed")
		if err := r.setPhaseDetail(ctx, &redisCluster, "Degraded", mastersReady, replicasReady,
			"UnresolvableMasterImbalance", rbErr.Error()); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	} else if acted {
		log.Info("Issued CLUSTER FAILOVER to correct a same-node master imbalance")
		// Give the failover a moment to complete before re-observing.
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// 7. Report observed roles (from the same data we already fetched
	// above -- no need to re-query Redis a second time).
	log.Info("Cluster healthy", "masters", mastersReady, "replicas", replicasReady)
	if err := r.setPhase(ctx, &redisCluster, "Ready", mastersReady, replicasReady); err != nil {
		return ctrl.Result{}, err
	}
	// Requeue periodically even when nothing changed at the Kubernetes
	// level -- master/replica role changes inside Redis are invisible to
	// K8s watches entirely, so without this the rebalance check above
	// would only ever fire once, right after a pod-recreation event, and
	// never again even if imbalance persisted.
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// podsUnschedulable reports whether any pod is stuck with
// PodScheduled=False / Unschedulable.
func podsUnschedulable(pods []corev1.Pod) (bool, string) {
	for _, pod := range pods {
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodScheduled &&
				cond.Status == corev1.ConditionFalse &&
				cond.Reason == corev1.PodReasonUnschedulable {
				msg := cond.Message
				if msg == "" {
					msg = "pod is unschedulable"
				}
				return true, fmt.Sprintf("%s: %s", pod.Name, msg)
			}
		}
	}
	return false, ""
}

// setPhase writes status only when something actually changed, so we don't
// generate a self-triggering write loop on every reconcile.
func (r *RedisClusterReconciler) setPhase(
	ctx context.Context, rc *cachev1.RedisCluster, phase string, masters, replicas int32,
) error {
	return r.setPhaseDetail(ctx, rc, phase, masters, replicas, "", "")
}

func (r *RedisClusterReconciler) setPhaseDetail(
	ctx context.Context, rc *cachev1.RedisCluster, phase string, masters, replicas int32,
	reason, message string,
) error {
	if !updateRedisClusterStatusDetail(rc, phase, masters, replicas, reason, message) {
		return nil
	}
	return r.Status().Update(ctx, rc)
}

func updateRedisClusterStatus(rc *cachev1.RedisCluster, phase string, masters, replicas int32) bool {
	return updateRedisClusterStatusDetail(rc, phase, masters, replicas, "", "")
}

func updateRedisClusterStatusDetail(
	rc *cachev1.RedisCluster, phase string, masters, replicas int32, reason, message string,
) bool {
	oldStatus := rc.Status
	oldStatus.Conditions = append([]metav1.Condition(nil), rc.Status.Conditions...)
	rc.Status.Phase = phase
	rc.Status.ReadyMasters = masters
	rc.Status.ReadyReplicas = replicas

	defaultReason, defaultMessage := phaseConditionDetails(phase)
	if reason == "" {
		reason = defaultReason
	}
	if message == "" {
		message = defaultMessage
	}
	ready := phase == "Ready"
	progressing := phase == "Provisioning" || phase == "Bootstrapping"
	degraded := phase == "Degraded" || phase == "Failed" || phase == "UnsupportedChange"
	conditions := []metav1.Condition{
		{
			Type:               "Ready",
			Status:             conditionStatus(ready),
			Reason:             reason,
			Message:            message,
			ObservedGeneration: rc.Generation,
			LastTransitionTime: metav1.Now(),
		},
		{
			Type:               "Progressing",
			Status:             conditionStatus(progressing),
			Reason:             conditionReason(progressing, "NotProgressing", reason),
			Message:            message,
			ObservedGeneration: rc.Generation,
			LastTransitionTime: metav1.Now(),
		},
		{
			Type:               "Degraded",
			Status:             conditionStatus(degraded),
			Reason:             conditionReason(degraded, reason, "NotDegraded"),
			Message:            message,
			ObservedGeneration: rc.Generation,
			LastTransitionTime: metav1.Now(),
		},
	}
	for _, condition := range conditions {
		apimeta.SetStatusCondition(&rc.Status.Conditions, condition)
	}

	return !reflect.DeepEqual(oldStatus, rc.Status)
}

func conditionStatus(active bool) metav1.ConditionStatus {
	if active {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

func conditionReason(active bool, activeReason, inactiveReason string) string {
	if active {
		return activeReason
	}
	return inactiveReason
}

func phaseConditionDetails(phase string) (reason, message string) {
	switch phase {
	case "Ready":
		return "ClusterReady", "Redis cluster is healthy"
	case "Provisioning":
		return "ResourcesProvisioning", "Waiting for Redis pods to become ready"
	case "Bootstrapping":
		return "ClusterBootstrapping", "Forming the Redis cluster"
	case "Degraded":
		return "ClusterDegraded", "Redis cluster views are not yet consistent"
	case "Failed":
		return "InvalidConfiguration", "RedisCluster configuration is invalid"
	case "UnsupportedChange":
		return "TopologyChangeUnsupported", "A Redis topology or storage change requires a migration that is not implemented"
	default:
		return "Reconciling", "Reconciling Redis cluster"
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1.RedisCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Named("rediscluster").
		Complete(r)
}
