/*
Copyright 2019 The KubeSphere Authors.

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

package app

import (
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog"
	"kubesphere.io/kubesphere/cmd/controller-manager/app/options"
	"kubesphere.io/kubesphere/pkg/controller/globalrolebinding"
	"kubesphere.io/kubesphere/pkg/controller/namespace"
	"kubesphere.io/kubesphere/pkg/controller/user"
	"kubesphere.io/kubesphere/pkg/models/kubeconfig"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"kubesphere.io/kubesphere/pkg/informers"
	"kubesphere.io/kubesphere/pkg/simple/client/k8s"
)

var allControllers = []string{
	"user",
	"user-sync-from-lldap",
	"namespace",
	"globalrolebinding",
}

// setup all available controllers one by one
func addAllControllers(mgr manager.Manager, client k8s.Client, informerFactory informers.InformerFactory,
	cmOptions *options.KubeSphereControllerManagerOptions,
	stopCh <-chan struct{}) error {

	kubernetesInformer := informerFactory.KubernetesSharedInformerFactory()
	kubesphereInformer := informerFactory.KubeSphereSharedInformerFactory()

	kubeconfigClient := kubeconfig.NewOperator(client.Kubernetes(),
		informerFactory.KubernetesSharedInformerFactory().Core().V1().ConfigMaps().Lister(),
		client.Config())

	// ldap user sync controller
	//if cmOptions.IsControllerEnabled("user-sync-from-lldap") {
	//	userSyncController := &user.SyncReconciler{
	//		MaxConcurrentReconciles: 1,
	//		//KubeconfigClient:        kubeconfigClient,
	//	}
	//	addControllerWithSetup(mgr, "user-sync-from-lldap", userSyncController)
	//}
	// "namespace" controller
	if cmOptions.IsControllerEnabled("namespace") {
		namespaceReconciler := &namespace.Reconciler{}
		addControllerWithSetup(mgr, "namespace", namespaceReconciler)
	}

	// "user" controller
	if cmOptions.IsControllerEnabled("user") {
		userController := &user.Reconciler{
			MaxConcurrentReconciles: 4,
			KubeconfigClient:        kubeconfigClient,
		}
		addControllerWithSetup(mgr, "user", userController)
	}

	// "globalrolebinding" controller
	if cmOptions.IsControllerEnabled("globalrolebinding") {
		globalRoleBindingController := globalrolebinding.NewController(client.Kubernetes(),
			kubesphereInformer.Iam().V1alpha2().GlobalRoleBindings(),
			kubernetesInformer.Apps().V1().Deployments(),
			kubernetesInformer.Core().V1().Pods(),
			kubesphereInformer.Iam().V1alpha2().Users(),
			"kubesphere/kubectl:v1.22.0")
		addController(mgr, "globalrolebinding", globalRoleBindingController)
	}
	// log all controllers process result
	for _, name := range allControllers {
		if cmOptions.IsControllerEnabled(name) {
			if addSuccessfullyControllers.Has(name) {
				klog.Infof("%s controller is enabled and added successfully.", name)
			} else {
				klog.Infof("%s controller is enabled but is not going to run due to its dependent component being disabled.", name)
			}
		} else {
			klog.Infof("%s controller is disabled by controller selectors.", name)
		}
	}

	return nil
}

var addSuccessfullyControllers = sets.NewString()

type setupableController interface {
	SetupWithManager(mgr ctrl.Manager) error
}

func addControllerWithSetup(mgr manager.Manager, name string, controller setupableController) {
	if err := controller.SetupWithManager(mgr); err != nil {
		klog.Fatalf("Unable to create %v controller: %v", name, err)
	}
	addSuccessfullyControllers.Insert(name)
}

func addController(mgr manager.Manager, name string, controller manager.Runnable) {
	if err := mgr.Add(controller); err != nil {
		klog.Fatalf("Unable to create %v controller: %v", name, err)
	}
	addSuccessfullyControllers.Insert(name)
}
