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
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

const redisClusterFinalizer = "cache.yourorg.io/pvc-cleanup"

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
				{Name: "redis", Port: 6379, TargetPort: intstr.FromInt32(6379)},
				{Name: "gossip", Port: 16379, TargetPort: intstr.FromInt32(16379)},
			},
		},
	}
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

// unsupportedStatefulSetChange returns a human-readable reason when the
// desired StatefulSet would require scale or PVC-template changes that this
// operator does not implement (no reshard / volume migration yet). Other
// pod-template drift is syncable and returns "".
func unsupportedStatefulSetChange(existing, desired *appsv1.StatefulSet) string {
	existingReplicas := int32(1)
	if existing.Spec.Replicas != nil {
		existingReplicas = *existing.Spec.Replicas
	}
	desiredReplicas := int32(1)
	if desired.Spec.Replicas != nil {
		desiredReplicas = *desired.Spec.Replicas
	}
	if existingReplicas != desiredReplicas {
		return fmt.Sprintf(
			"scaling StatefulSet replicas from %d to %d is not supported (no reshard path yet); recreate the RedisCluster or restore Spec.Nodes/ReplicasPerNode",
			existingReplicas, desiredReplicas)
	}
	if pvcStorageChanged(existing.Spec.VolumeClaimTemplates, desired.Spec.VolumeClaimTemplates) {
		return "changing storageSize/storageClassName after creation is not supported (PVC template is immutable); recreate the RedisCluster to change storage"
	}
	return ""
}

// pvcStorageChanged compares only the storage fields we own. Full
// DeepEqual on VolumeClaimTemplates is too noisy -- the API server fills
// defaults (volumeMode, etc.) on the live object that our desired builder
// omits, which would falsely trip "unsupported change" every reconcile.
func pvcStorageChanged(existing, desired []corev1.PersistentVolumeClaim) bool {
	if len(existing) != len(desired) {
		return true
	}
	for i := range desired {
		eReq := existing[i].Spec.Resources.Requests[corev1.ResourceStorage]
		dReq := desired[i].Spec.Resources.Requests[corev1.ResourceStorage]
		if !eReq.Equal(dReq) {
			return true
		}
		eSC, dSC := "", ""
		if existing[i].Spec.StorageClassName != nil {
			eSC = *existing[i].Spec.StorageClassName
		}
		if desired[i].Spec.StorageClassName != nil {
			dSC = *desired[i].Spec.StorageClassName
		}
		if eSC != dSC {
			return true
		}
	}
	return false
}

// syncHeadlessService updates labels/selector/ports on an existing headless
// Service when they drift from desired. ClusterIP is left alone (immutable).
func syncHeadlessService(existing *corev1.Service, desired *corev1.Service) bool {
	changed := false
	if !reflect.DeepEqual(existing.Labels, desired.Labels) {
		existing.Labels = desired.Labels
		changed = true
	}
	if !reflect.DeepEqual(existing.Spec.Selector, desired.Spec.Selector) {
		existing.Spec.Selector = desired.Spec.Selector
		changed = true
	}
	if !reflect.DeepEqual(existing.Spec.Ports, desired.Spec.Ports) {
		existing.Spec.Ports = desired.Spec.Ports
		changed = true
	}
	return changed
}

// syncStatefulSetTemplate copies mutable pod-template fields we own from
// desired onto existing. Only compares explicit fields (image, resources,
// command, topology spread) -- full Template DeepEqual fights API-server
// defaulting and causes perpetual update loops.
func syncStatefulSetTemplate(existing, desired *appsv1.StatefulSet) bool {
	changed := false
	if !reflect.DeepEqual(existing.Labels, desired.Labels) {
		existing.Labels = desired.Labels
		changed = true
	}
	eC := redisContainer(&existing.Spec.Template.Spec)
	dC := redisContainer(&desired.Spec.Template.Spec)
	if eC != nil && dC != nil {
		if eC.Image != dC.Image {
			eC.Image = dC.Image
			changed = true
		}
		if !reflect.DeepEqual(eC.Resources, dC.Resources) {
			eC.Resources = dC.Resources
			changed = true
		}
		if !reflect.DeepEqual(eC.Command, dC.Command) {
			eC.Command = append([]string(nil), dC.Command...)
			changed = true
		}
	}
	if !reflect.DeepEqual(
		existing.Spec.Template.Spec.TopologySpreadConstraints,
		desired.Spec.Template.Spec.TopologySpreadConstraints,
	) {
		existing.Spec.Template.Spec.TopologySpreadConstraints =
			desired.Spec.Template.Spec.TopologySpreadConstraints
		changed = true
	}
	return changed
}

func redisContainer(spec *corev1.PodSpec) *corev1.Container {
	for i := range spec.Containers {
		if spec.Containers[i].Name == "redis" {
			return &spec.Containers[i]
		}
	}
	return nil
}

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *RedisClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var redisCluster cachev1.RedisCluster
	if err := r.Get(ctx, req.NamespacedName, &redisCluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.Info("Reconciling RedisCluster",
		"name", redisCluster.Name,
		"nodes", redisCluster.Spec.Nodes,
		"replicasPerNode", redisCluster.Spec.ReplicasPerNode,
	)

	// Deletion: clean up PVCs owned by this cluster, then drop the finalizer.
	// StatefulSet owner-refs do not delete volumeClaimTemplate PVCs by default.
	if !redisCluster.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &redisCluster)
	}
	if !containsString(redisCluster.Finalizers, redisClusterFinalizer) {
		redisCluster.Finalizers = append(redisCluster.Finalizers, redisClusterFinalizer)
		if err := r.Update(ctx, &redisCluster); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// 1. Headless Service -- create if missing, sync labels/ports if drifted.
	svc := desiredHeadlessService(&redisCluster)
	if err := ctrl.SetControllerReference(&redisCluster, svc, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	var existingSvc corev1.Service
	if err := r.Get(ctx, client.ObjectKeyFromObject(svc), &existingSvc); apierrors.IsNotFound(err) {
		log.Info("Creating headless Service", "name", svc.Name)
		if err := r.Create(ctx, svc); err != nil {
			return ctrl.Result{}, fmt.Errorf("creating headless service: %w", err)
		}
	} else if err != nil {
		return ctrl.Result{}, err
	} else if syncHeadlessService(&existingSvc, svc) {
		log.Info("Updating headless Service", "name", existingSvc.Name)
		if err := r.Update(ctx, &existingSvc); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating headless service: %w", err)
		}
	}

	// 2. StatefulSet -- create if missing; reject scale/storage changes;
	// sync pod template (image/resources/flags) when safe.
	sts, err := desiredStatefulSet(&redisCluster)
	if err != nil {
		// A bad spec (e.g. unparseable storageSize) isn't something
		// retrying will fix -- surface it in status instead of
		// requeuing forever.
		_ = r.setPhase(ctx, &redisCluster, "Failed", 0, 0)
		return ctrl.Result{}, err
	}
	if err := ctrl.SetControllerReference(&redisCluster, sts, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	var existingSts appsv1.StatefulSet
	if err := r.Get(ctx, client.ObjectKeyFromObject(sts), &existingSts); apierrors.IsNotFound(err) {
		log.Info("Creating StatefulSet", "name", sts.Name, "replicas", *sts.Spec.Replicas)
		if err := r.Create(ctx, sts); err != nil {
			return ctrl.Result{}, fmt.Errorf("creating statefulset: %w", err)
		}
		existingSts = *sts
	} else if err != nil {
		return ctrl.Result{}, err
	} else if reason := unsupportedStatefulSetChange(&existingSts, sts); reason != "" {
		log.Info("Rejecting unsupported StatefulSet change", "reason", reason)
		_ = r.setPhase(ctx, &redisCluster, "Failed", 0, 0)
		// No requeue -- retrying will not make scale/storage supported.
		return ctrl.Result{}, nil
	} else if syncStatefulSetTemplate(&existingSts, sts) {
		log.Info("Updating StatefulSet pod template", "name", existingSts.Name)
		if err := r.Update(ctx, &existingSts); err != nil {
			return ctrl.Result{}, fmt.Errorf("updating statefulset: %w", err)
		}
		// Rolling update in progress; wait for readiness on next pass.
		return ctrl.Result{RequeueAfter: 5 * time.Second}, r.setPhase(ctx, &redisCluster, "Provisioning", 0, 0)
	}

	// 3. Nothing to do at the Redis level until every pod is actually
	// running -- MEET against a pod that has no IP yet just fails.
	// Compare against the live STS replica count (scale is rejected above,
	// so this matches the CR when the cluster is in a supported state).
	desiredReady := int32(0)
	if existingSts.Spec.Replicas != nil {
		desiredReady = *existingSts.Spec.Replicas
	}
	if existingSts.Status.ReadyReplicas != desiredReady {
		log.Info("Waiting for all pods to be ready",
			"ready", existingSts.Status.ReadyReplicas, "desired", desiredReady)
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

	nodes := make([]nodeInfo, 0, len(podList.Items))
	for _, pod := range podList.Items {
		if pod.Status.PodIP == "" || pod.Spec.NodeName == "" {
			// Still settling; come back shortly rather than acting on a
			// half-known topology.
			log.Info("pod not fully scheduled yet, requeueing", "pod", pod.Name)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		nodes = append(nodes, nodeInfo{
			PodName: pod.Name,
			K8sNode: pod.Spec.NodeName,
			IP:      pod.Status.PodIP,
		})
	}
	// Deterministic order so repeated reconciles behave identically.
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].PodName < nodes[j].PodName })

	// 5. Bootstrap (or resume) the Redis cluster until the full slot map
	// is assigned and planned replicas are attached. needsBootstrap is
	// true for empty AND partial formation; bootstrapCluster itself is
	// idempotent so a mid-flight failure can recover on the next cycle.
	numMasters := int(redisCluster.Spec.Nodes)
	if needsBootstrap(ctx, nodes, numMasters) {
		log.Info("Bootstrapping Redis cluster", "pods", len(nodes), "masters", numMasters)
		if err := bootstrapCluster(ctx, nodes, numMasters); err != nil {
			if errors.Is(err, ErrInsufficientNodes) {
				log.Error(err, "Bootstrap blocked by cluster topology")
				_ = r.setPhase(ctx, &redisCluster, "Failed", 0, 0)
				// Requeue slowly: adding worker nodes can make this recoverable.
				return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
			}
			log.Error(err, "Bootstrap failed, will retry")
			_ = r.setPhase(ctx, &redisCluster, "Bootstrapping", 0, 0)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		log.Info("Bootstrap complete")
		// Give gossip a moment to settle before reporting roles.
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// 5b. Drop Redis gossip entries for pods that no longer exist. Ghosts
	// inflate CLUSTER NODES line counts and trip fail flags permanently.
	if purged, err := purgeGhostNodes(ctx, nodes); err != nil {
		log.Error(err, "Ghost node purge failed")
	} else if purged > 0 {
		log.Info("Purged ghost Redis nodes", "count", purged)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// 6. Cluster is formed. Require every pod's view to pass completeness
	// checks before trusting roles for status or rebalance.
	assessment := assessHealth(ctx, nodes, len(nodes))
	if !assessment.Complete {
		log.Info("cluster view not yet consistent across all pods, will retry",
			"queriedPod", assessment.QueriedPod, "issues", assessment.Issues)
		_ = r.setPhase(ctx, &redisCluster, "Degraded", 0, 0)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	byIP := map[string]*nodeInfo{}
	for i := range nodes {
		byIP[nodes[i].IP] = &nodes[i]
	}
	parseClusterNodesText(assessment.RawNodes, byIP)

	// Only ever act on a view every pod agrees is complete --
	// rebalanceMasters must never fire against a partial/stale view,
	// which could misdiagnose a real imbalance or miss one entirely.
	if acted, rbErr := rebalanceMasters(ctx, nodes); rbErr != nil {
		log.Error(rbErr, "Master rebalance check failed")
		// Unresolvable imbalance (or failover error) is not Ready.
		_ = r.setPhase(ctx, &redisCluster, "Degraded", 0, 0)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	} else if acted {
		log.Info("Issued CLUSTER FAILOVER to correct a same-node master imbalance")
		// Give the failover a moment to complete before re-observing.
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// 7. Report observed roles (from the same data we already fetched
	// above -- no need to re-query Redis a second time).
	var mastersReady, replicasReady int32
	for _, n := range nodes {
		if n.NodeID == "" {
			continue
		}
		if n.IsMaster {
			mastersReady++
		} else {
			replicasReady++
		}
	}
	log.Info("cluster healthy", "masters", mastersReady, "replicas", replicasReady)
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

// setPhase writes status only when something actually changed, so we don't
// generate a self-triggering write loop on every reconcile.
func (r *RedisClusterReconciler) setPhase(
	ctx context.Context, rc *cachev1.RedisCluster, phase string, masters, replicas int32,
) error {
	if rc.Status.Phase == phase &&
		rc.Status.ReadyMasters == masters &&
		rc.Status.ReadyReplicas == replicas {
		return nil
	}
	rc.Status.Phase = phase
	rc.Status.ReadyMasters = masters
	rc.Status.ReadyReplicas = replicas
	return r.Status().Update(ctx, rc)
}

func (r *RedisClusterReconciler) reconcileDelete(ctx context.Context, rc *cachev1.RedisCluster) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	if !containsString(rc.Finalizers, redisClusterFinalizer) {
		return ctrl.Result{}, nil
	}

	var pvcList corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &pvcList, client.InNamespace(rc.Namespace)); err != nil {
		return ctrl.Result{}, fmt.Errorf("listing PVCs for cleanup: %w", err)
	}
	prefix := "data-" + rc.Name + "-"
	outstanding := false
	for i := range pvcList.Items {
		pvc := &pvcList.Items[i]
		if !strings.HasPrefix(pvc.Name, prefix) {
			continue
		}
		if !pvc.DeletionTimestamp.IsZero() {
			outstanding = true
			continue
		}
		log.Info("Deleting PVC", "name", pvc.Name)
		if err := r.Delete(ctx, pvc); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("deleting PVC %s: %w", pvc.Name, err)
		}
		outstanding = true
	}
	if outstanding {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	rc.Finalizers = removeString(rc.Finalizers, redisClusterFinalizer)
	if err := r.Update(ctx, rc); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

func removeString(slice []string, s string) []string {
	out := slice[:0]
	for _, item := range slice {
		if item != s {
			out = append(out, item)
		}
	}
	return out
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
