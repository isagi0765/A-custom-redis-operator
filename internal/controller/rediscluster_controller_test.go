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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1 "yourorg.io/redis-cluster-operator/api/v1"
)

var _ = Describe("RedisCluster Controller", func() {
	Context("When reconciling a resource", func() {
		const (
			resourceName      = "test-resource"
			resourceNamespace = "default"
		)

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: resourceNamespace,
		}
		rediscluster := &cachev1.RedisCluster{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind RedisCluster")
			err := k8sClient.Get(ctx, typeNamespacedName, rediscluster)
			if err != nil && errors.IsNotFound(err) {
				resource := &cachev1.RedisCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: resourceNamespace,
					},
					Spec: cachev1.RedisClusterSpec{
						Nodes:           3,
						ReplicasPerNode: 1,
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &cachev1.RedisCluster{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance RedisCluster")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &RedisClusterReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			initialSts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, initialSts)).To(Succeed())
			initialService := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      resourceName + "-headless",
				Namespace: resourceNamespace,
			}, initialService)).To(Succeed())

			By("reconciling again without mistaking API defaults for unsupported changes")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			updated := &cachev1.RedisCluster{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal("Provisioning"))
			currentSts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, currentSts)).To(Succeed())
			Expect(currentSts.Generation).To(Equal(initialSts.Generation))
			currentService := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      resourceName + "-headless",
				Namespace: resourceNamespace,
			}, currentService)).To(Succeed())
			Expect(currentService.Generation).To(Equal(initialService.Generation))
		})
	})
})
