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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSlotRanges(t *testing.T) {
	ranges := slotRanges(3)
	if len(ranges) != 3 {
		t.Fatalf("expected 3 ranges, got %d", len(ranges))
	}
	if ranges[0] != [2]int{0, 5460} {
		t.Errorf("range 0 = %v", ranges[0])
	}
	if ranges[1] != [2]int{5461, 10921} {
		t.Errorf("range 1 = %v", ranges[1])
	}
	if ranges[2] != [2]int{10922, 16383} {
		t.Errorf("range 2 = %v", ranges[2])
	}
	total := 0
	for _, r := range ranges {
		total += r[1] - r[0] + 1
	}
	if total != totalSlots {
		t.Errorf("coverage %d, want %d", total, totalSlots)
	}
}

func TestPlanRolesAntiAffinity(t *testing.T) {
	nodes := []nodeInfo{
		{PodName: "rc-0", K8sNode: "worker1", IP: "10.0.0.1"},
		{PodName: "rc-1", K8sNode: "worker1", IP: "10.0.0.2"},
		{PodName: "rc-2", K8sNode: "worker2", IP: "10.0.0.3"},
		{PodName: "rc-3", K8sNode: "worker2", IP: "10.0.0.4"},
		{PodName: "rc-4", K8sNode: "worker3", IP: "10.0.0.5"},
		{PodName: "rc-5", K8sNode: "worker3", IP: "10.0.0.6"},
	}
	masters, replicaOf, err := planRoles(nodes, 3)
	if err != nil {
		t.Fatalf("planRoles: %v", err)
	}
	if len(masters) != 3 {
		t.Fatalf("expected 3 masters, got %d", len(masters))
	}
	masterByPod := map[string]*nodeInfo{}
	for _, m := range masters {
		masterByPod[m.PodName] = m
	}
	for replicaPod, masterPod := range replicaOf {
		var replica *nodeInfo
		for i := range nodes {
			if nodes[i].PodName == replicaPod {
				replica = &nodes[i]
				break
			}
		}
		master := masterByPod[masterPod]
		if replica == nil || master == nil {
			t.Fatalf("missing pair %s -> %s", replicaPod, masterPod)
		}
		if replica.K8sNode == master.K8sNode {
			t.Errorf("replica %s shares node %s with master %s",
				replicaPod, replica.K8sNode, masterPod)
		}
	}
}

func TestPlanRolesInsufficientNodes(t *testing.T) {
	nodes := []nodeInfo{
		{PodName: "rc-0", K8sNode: "worker1", IP: "10.0.0.1"},
		{PodName: "rc-1", K8sNode: "worker1", IP: "10.0.0.2"},
		{PodName: "rc-2", K8sNode: "worker2", IP: "10.0.0.3"},
		{PodName: "rc-3", K8sNode: "worker2", IP: "10.0.0.4"},
	}
	_, _, err := planRoles(nodes, 3)
	if !errors.Is(err, ErrInsufficientNodes) {
		t.Fatalf("expected ErrInsufficientNodes, got %v", err)
	}
}

func TestIsClusterViewComplete(t *testing.T) {
	good := "" +
		"aaa 10.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-5460\n" +
		"bbb 10.0.0.2:6379@16379 master - 0 0 2 connected 5461-10921\n" +
		"ccc 10.0.0.3:6379@16379 master - 0 0 3 connected 10922-16383\n" +
		"ddd 10.0.0.4:6379@16379 slave aaa 0 0 1 connected\n" +
		"eee 10.0.0.5:6379@16379 slave bbb 0 0 2 connected\n" +
		"fff 10.0.0.6:6379@16379 slave ccc 0 0 3 connected\n"
	if ok, reason := isClusterViewComplete(good, 6); !ok {
		t.Fatalf("expected complete, got %s", reason)
	}

	partial := "" +
		"aaa 10.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-5460\n" +
		"bbb 10.0.0.2:6379@16379 master - 0 0 2 connected 5461-10921\n"
	if ok, _ := isClusterViewComplete(partial, 6); ok {
		t.Fatal("expected incomplete for partial node list")
	}

	unresolved := "" +
		"aaa ?:6379@16379 myself,master - 0 0 1 connected 0-5460\n" +
		"bbb 10.0.0.2:6379@16379 master - 0 0 2 connected 5461-10921\n" +
		"ccc 10.0.0.3:6379@16379 master - 0 0 3 connected 10922-16383\n" +
		"ddd 10.0.0.4:6379@16379 slave aaa 0 0 1 connected\n" +
		"eee 10.0.0.5:6379@16379 slave bbb 0 0 2 connected\n" +
		"fff 10.0.0.6:6379@16379 slave ccc 0 0 3 connected\n"
	if ok, reason := isClusterViewComplete(unresolved, 6); ok {
		t.Fatal("expected incomplete for unresolved address")
	} else if reason == "" {
		t.Fatal("expected reason for unresolved address")
	}

	failed := "" +
		"aaa 10.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-5460\n" +
		"bbb 10.0.0.2:6379@16379 master,fail - 0 0 2 connected 5461-10921\n" +
		"ccc 10.0.0.3:6379@16379 master - 0 0 3 connected 10922-16383\n" +
		"ddd 10.0.0.4:6379@16379 slave aaa 0 0 1 connected\n" +
		"eee 10.0.0.5:6379@16379 slave bbb 0 0 2 connected\n" +
		"fff 10.0.0.6:6379@16379 slave ccc 0 0 3 connected\n"
	if ok, _ := isClusterViewComplete(failed, 6); ok {
		t.Fatal("expected incomplete for fail flag")
	}
}

func TestParseClusterNodesText(t *testing.T) {
	byIP := map[string]*nodeInfo{
		"10.0.0.1": {PodName: "rc-0", IP: "10.0.0.1"},
		"10.0.0.2": {PodName: "rc-1", IP: "10.0.0.2"},
	}
	raw := "" +
		"aaa 10.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-100\n" +
		"bbb 10.0.0.2:6379@16379 slave aaa 0 0 1 connected\n" +
		"ghost 10.0.0.9:6379@16379 master,fail - 0 0 0 connected\n"
	parseClusterNodesText(raw, byIP)
	if byIP["10.0.0.1"].NodeID != "aaa" || !byIP["10.0.0.1"].IsMaster || !byIP["10.0.0.1"].HasSlots {
		t.Errorf("master parse = %+v", byIP["10.0.0.1"])
	}
	if byIP["10.0.0.2"].IsMaster || byIP["10.0.0.2"].MasterID != "aaa" {
		t.Errorf("replica parse = %+v", byIP["10.0.0.2"])
	}
}

func TestUnsupportedStatefulSetChange(t *testing.T) {
	replicas := int32(6)
	existing := &appsv1.StatefulSet{
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "data"},
					Spec: corev1.PersistentVolumeClaimSpec{
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Gi"),
							},
						},
					},
				},
			},
		},
	}
	desiredSame := existing.DeepCopy()
	if reason := unsupportedStatefulSetChange(existing, desiredSame); reason != "" {
		t.Fatalf("expected no change, got %q", reason)
	}

	scaled := int32(9)
	desiredScale := existing.DeepCopy()
	desiredScale.Spec.Replicas = &scaled
	if reason := unsupportedStatefulSetChange(existing, desiredScale); reason == "" {
		t.Fatal("expected scale rejection")
	}

	desiredStorage := existing.DeepCopy()
	desiredStorage.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage] =
		resource.MustParse("2Gi")
	if reason := unsupportedStatefulSetChange(existing, desiredStorage); reason == "" {
		t.Fatal("expected storage rejection")
	}
}
