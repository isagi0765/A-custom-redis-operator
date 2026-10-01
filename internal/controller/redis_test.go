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
	"errors"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1 "yourorg.io/redis-cluster-operator/api/v1"
)

const completeClusterView = `master-one 10.0.0.1:6379@16379 master - 0 0 1 connected 0-8191
master-two 10.0.0.2:6379@16379 master - 0 0 2 connected 8192-16383`

func TestAssessClusterViewsRequiresEveryPodViewToBeComplete(t *testing.T) {
	partialView := `master-one 10.0.0.1:6379@16379 master - 0 0 1 connected 0-8191`

	tests := []struct {
		name         string
		views        []clusterView
		wantComplete bool
		wantPod      string
		wantIssues   int
	}{
		{
			name: "all views complete",
			views: []clusterView{
				{PodName: "redis-0", Raw: completeClusterView},
				{PodName: "redis-1", Raw: completeClusterView},
			},
			wantComplete: true,
			wantPod:      "redis-0",
		},
		{
			name: "one complete view cannot hide stale gossip",
			views: []clusterView{
				{PodName: "redis-0", Raw: completeClusterView},
				{PodName: "redis-1", Raw: partialView},
			},
			wantPod:    "redis-0",
			wantIssues: 1,
		},
		{
			name: "unreachable pod prevents readiness",
			views: []clusterView{
				{PodName: "redis-0", Raw: completeClusterView},
				{PodName: "redis-1", Err: errors.New("connection refused")},
			},
			wantPod:    "redis-0",
			wantIssues: 1,
		},
		{
			name: "empty view set is not ready",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := assessClusterViews(tt.views, 2)
			if got.Complete != tt.wantComplete {
				t.Errorf("Complete = %t, want %t", got.Complete, tt.wantComplete)
			}
			if got.QueriedPod != tt.wantPod {
				t.Errorf("QueriedPod = %q, want %q", got.QueriedPod, tt.wantPod)
			}
			if len(got.Issues) != tt.wantIssues {
				t.Errorf("len(Issues) = %d, want %d: %v", len(got.Issues), tt.wantIssues, got.Issues)
			}
		})
	}
}

func TestUpdateRedisClusterStatusSetsStandardConditions(t *testing.T) {
	tests := []struct {
		phase       string
		ready       metav1.ConditionStatus
		progressing metav1.ConditionStatus
		degraded    metav1.ConditionStatus
	}{
		{phase: "Ready", ready: metav1.ConditionTrue, progressing: metav1.ConditionFalse, degraded: metav1.ConditionFalse},
		{phase: "Provisioning", ready: metav1.ConditionFalse, progressing: metav1.ConditionTrue, degraded: metav1.ConditionFalse},
		{phase: "Degraded", ready: metav1.ConditionFalse, progressing: metav1.ConditionFalse, degraded: metav1.ConditionTrue},
		{phase: "Failed", ready: metav1.ConditionFalse, progressing: metav1.ConditionFalse, degraded: metav1.ConditionTrue},
		{phase: "UnsupportedChange", ready: metav1.ConditionFalse, progressing: metav1.ConditionFalse, degraded: metav1.ConditionTrue},
	}

	for _, tt := range tests {
		t.Run(tt.phase, func(t *testing.T) {
			rc := &cachev1.RedisCluster{}
			if !updateRedisClusterStatus(rc, tt.phase, 3, 3) {
				t.Fatal("first status update reported no changes")
			}

			conditions := make(map[string]metav1.Condition, len(rc.Status.Conditions))
			for _, condition := range rc.Status.Conditions {
				conditions[condition.Type] = condition
				if condition.ObservedGeneration != rc.Generation {
					t.Errorf("%s observedGeneration = %d, want %d", condition.Type, condition.ObservedGeneration, rc.Generation)
				}
			}

			if len(conditions) != 3 {
				t.Fatalf("got %d conditions, want Ready, Progressing, and Degraded", len(conditions))
			}

			for conditionType, want := range map[string]metav1.ConditionStatus{
				"Ready":       tt.ready,
				"Progressing": tt.progressing,
				"Degraded":    tt.degraded,
			} {
				if got := conditions[conditionType].Status; got != want {
					t.Errorf("%s status = %s, want %s", conditionType, got, want)
				}
			}

			transitionTime := conditions["Ready"].LastTransitionTime
			if updateRedisClusterStatus(rc, tt.phase, 3, 3) {
				t.Fatal("identical status update reported changes")
			}
			for _, condition := range rc.Status.Conditions {
				if condition.Type == "Ready" && !condition.LastTransitionTime.Equal(&transitionTime) {
					t.Errorf("unchanged Ready condition transition time changed from %s to %s", transitionTime, condition.LastTransitionTime)
				}
			}
		})
	}
}

func TestSyncHeadlessServicePreservesAllocatedFields(t *testing.T) {
	existing := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"old": "label"}},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  map[string]string{"old": "selector"},
			Ports:     []corev1.ServicePort{{Name: "old", Port: 1}},
		},
	}
	desired := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "redis-cluster"}},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "redis-cluster"},
			Ports:    []corev1.ServicePort{{Name: "redis", Port: 6379}},
		},
	}

	if !syncHeadlessService(existing, desired) {
		t.Fatal("expected service drift to be reconciled")
	}
	if existing.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Errorf("ClusterIP = %q, want %q", existing.Spec.ClusterIP, corev1.ClusterIPNone)
	}
	if syncHeadlessService(existing, desired) {
		t.Fatal("identical service sync reported changes")
	}
}

func TestStatefulSetReconciliationPreservesTopologyAndDetectsUnsupportedChanges(t *testing.T) {
	replicas := int32(6)
	existing := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"old": "label"}},
		Spec: appsv1.StatefulSetSpec{
			Replicas: replicasPointer(replicas),
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "redis", Image: "redis:old"}},
			}},
		},
	}
	desired := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "redis-cluster"}},
		Spec: appsv1.StatefulSetSpec{
			Replicas: replicasPointer(replicas),
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "redis", Image: "redis:new"}},
			}},
		},
	}

	if reason := statefulSetUnsupportedChange(existing, desired); reason != "" {
		t.Fatalf("unexpected unsupported change: %s", reason)
	}
	if !syncStatefulSetTemplate(existing, desired) {
		t.Fatal("expected pod-template drift to be reconciled")
	}
	if *existing.Spec.Replicas != replicas {
		t.Errorf("replicas changed to %d, want %d", *existing.Spec.Replicas, replicas)
	}
	if existing.Spec.Template.Spec.Containers[0].Image != "redis:new" {
		t.Errorf("image = %q, want redis:new", existing.Spec.Template.Spec.Containers[0].Image)
	}
	if syncStatefulSetTemplate(existing, desired) {
		t.Fatal("identical StatefulSet template sync reported changes")
	}

	scaled := desired.DeepCopy()
	*scaled.Spec.Replicas = replicas + 1
	if reason := statefulSetUnsupportedChange(existing, scaled); reason == "" {
		t.Fatal("replica-count change was not rejected")
	}

	storageChanged := desired.DeepCopy()
	storageChanged.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "data"}}}
	if reason := statefulSetUnsupportedChange(existing, storageChanged); reason == "" {
		t.Fatal("storage-template change was not rejected")
	}
}

func TestBootstrapFormationIncompleteAndIdempotentHelpers(t *testing.T) {
	t.Run("incomplete when slots missing", func(t *testing.T) {
		nodes := []nodeInfo{
			{PodName: "r0", IsMaster: true, HasSlots: true, NodeID: "m1"},
			{PodName: "r1", IsMaster: true, HasSlots: false, NodeID: "m2"},
			{PodName: "r2", MasterID: "m1", NodeID: "s1"},
		}
		if !bootstrapFormationIncomplete(nodes, 2, 8000) {
			t.Fatal("expected incomplete when assignedSlots < 16384")
		}
	})

	t.Run("incomplete when replica not linked", func(t *testing.T) {
		nodes := []nodeInfo{
			{PodName: "r0", IsMaster: true, HasSlots: true, NodeID: "m1"},
			{PodName: "r1", IsMaster: true, HasSlots: true, NodeID: "m2"},
			{PodName: "r2", IsMaster: true, HasSlots: false, NodeID: "empty"},
			{PodName: "r3", MasterID: "m1", NodeID: "s1"},
		}
		if !bootstrapFormationIncomplete(nodes, 2, totalSlots) {
			t.Fatal("expected incomplete when a non-master slot owner count is short on replicas")
		}
	})

	t.Run("complete after full formation even if extra empty master absent", func(t *testing.T) {
		nodes := []nodeInfo{
			{PodName: "r0", IsMaster: true, HasSlots: true, NodeID: "m1"},
			{PodName: "r1", IsMaster: true, HasSlots: true, NodeID: "m2"},
			{PodName: "r2", MasterID: "m1", NodeID: "s1"},
			{PodName: "r3", MasterID: "m2", NodeID: "s2"},
		}
		if bootstrapFormationIncomplete(nodes, 2, totalSlots) {
			t.Fatal("expected formation complete")
		}
	})

	t.Run("slot assignment skip", func(t *testing.T) {
		if nodeNeedsSlotAssignment(&nodeInfo{HasSlots: true}) {
			t.Fatal("slot-owning master should not need assignment")
		}
		if !nodeNeedsSlotAssignment(&nodeInfo{HasSlots: false}) {
			t.Fatal("empty master should need assignment")
		}
	})

	t.Run("replicate skip rules", func(t *testing.T) {
		master := &nodeInfo{NodeID: "m1"}
		if nodeNeedsReplicate(&nodeInfo{IsMaster: true, HasSlots: true}, master) {
			t.Fatal("must not demote slot-owning master")
		}
		if nodeNeedsReplicate(&nodeInfo{MasterID: "m1"}, master) {
			t.Fatal("already-linked replica should not need replicate")
		}
		if !nodeNeedsReplicate(&nodeInfo{IsMaster: true, HasSlots: false}, master) {
			t.Fatal("empty master planned as replica should need replicate")
		}
		if !nodeNeedsReplicate(&nodeInfo{MasterID: "other"}, master) {
			t.Fatal("wrong master link should need replicate")
		}
	})
}

func TestRolesMatchSpec(t *testing.T) {
	nodes := []nodeInfo{
		{NodeID: "m1", IsMaster: true, HasSlots: true},
		{NodeID: "m2", IsMaster: true, HasSlots: true},
		{NodeID: "m3", IsMaster: true, HasSlots: true},
		{NodeID: "s1", MasterID: "m1"},
		{NodeID: "s2", MasterID: "m2"},
		{NodeID: "s3", MasterID: "m3"},
	}
	if !rolesMatchSpec(nodes, 3, 1) {
		t.Fatal("expected matching 3 masters / 3 replicas")
	}
	if rolesMatchSpec(nodes[:5], 3, 1) {
		t.Fatal("expected mismatch when a replica is missing")
	}
	mastersOnly := []nodeInfo{
		{NodeID: "m1", IsMaster: true, HasSlots: true},
		{NodeID: "m2", IsMaster: true, HasSlots: true},
		{NodeID: "m3", IsMaster: true, HasSlots: false},
	}
	if rolesMatchSpec(mastersOnly, 3, 1) {
		t.Fatal("empty master without slots must not satisfy Spec.Nodes")
	}
}

func TestPodsUnschedulable(t *testing.T) {
	ok := []corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-0"},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
			Type:   corev1.PodScheduled,
			Status: corev1.ConditionTrue,
		}}},
	}}
	if unsched, _ := podsUnschedulable(ok); unsched {
		t.Fatal("scheduled pod should not be reported unschedulable")
	}

	stuck := []corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-1"},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
			Type:    corev1.PodScheduled,
			Status:  corev1.ConditionFalse,
			Reason:  corev1.PodReasonUnschedulable,
			Message: "0/3 nodes available",
		}}},
	}}
	unsched, msg := podsUnschedulable(stuck)
	if !unsched {
		t.Fatal("expected unschedulable pod")
	}
	if msg == "" || !strings.Contains(msg, "redis-1") {
		t.Fatalf("unexpected message %q", msg)
	}
}

func TestPlanMembershipHeal(t *testing.T) {
	raw := `aaaa 10.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-8191
bbbb 10.0.0.99:6379@16379 master,fail - 0 0 2 connected
cccc 10.0.0.2:6379@16379 slave aaaa 0 0 1 connected`
	current := map[string]bool{"10.0.0.1": true, "10.0.0.2": true, "10.0.0.3": true}
	plan := planMembershipHeal(raw, current)
	if len(plan.ForgetNodeIDs) != 1 || plan.ForgetNodeIDs[0] != "bbbb" {
		t.Fatalf("ForgetNodeIDs = %v, want [bbbb]", plan.ForgetNodeIDs)
	}
	if len(plan.MeetIPs) != 1 || plan.MeetIPs[0] != "10.0.0.3" {
		t.Fatalf("MeetIPs = %v, want [10.0.0.3]", plan.MeetIPs)
	}
}

func TestUpdateRedisClusterStatusDetailOverridesReason(t *testing.T) {
	rc := &cachev1.RedisCluster{}
	if !updateRedisClusterStatusDetail(rc, "Degraded", 2, 1, "RoleMismatch", "masters short") {
		t.Fatal("expected status change")
	}
	var degraded metav1.Condition
	for _, c := range rc.Status.Conditions {
		if c.Type == "Degraded" {
			degraded = c
		}
	}
	if degraded.Reason != "RoleMismatch" {
		t.Fatalf("Degraded reason = %q, want RoleMismatch", degraded.Reason)
	}
	if degraded.Message != "masters short" {
		t.Fatalf("Degraded message = %q", degraded.Message)
	}

	rc2 := &cachev1.RedisCluster{}
	updateRedisClusterStatusDetail(rc2, "Failed", 0, 0, "InsufficientNodes", "need 3 nodes")
	var failed metav1.Condition
	for _, c := range rc2.Status.Conditions {
		if c.Type == "Degraded" {
			failed = c
		}
	}
	if failed.Reason != "InsufficientNodes" {
		t.Fatalf("Failed/Degraded reason = %q, want InsufficientNodes", failed.Reason)
	}

	rc3 := &cachev1.RedisCluster{}
	updateRedisClusterStatusDetail(rc3, "Degraded", 3, 3, "UnresolvableMasterImbalance", "no safe move")
	for _, c := range rc3.Status.Conditions {
		if c.Type == "Degraded" && c.Reason != "UnresolvableMasterImbalance" {
			t.Fatalf("reason = %q", c.Reason)
		}
	}
}

func replicasPointer(replicas int32) *int32 {
	return &replicas
}
