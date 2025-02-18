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

package user

import (
	"context"
	"fmt"
	"github.com/beclab/lldap-client/pkg/cache/memory"
	lconfig "github.com/beclab/lldap-client/pkg/config"
	lapierrors "github.com/beclab/lldap-client/pkg/errors"
	"github.com/beclab/lldap-client/pkg/generated"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"
	iamv1alpha2 "kubesphere.io/api/iam/v1alpha2"
	"kubesphere.io/kubesphere/pkg/constants"
	"kubesphere.io/kubesphere/pkg/models/kubeconfig"
	"kubesphere.io/kubesphere/pkg/utils/sliceutil"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"strconv"
	"time"

	lclient "github.com/beclab/lldap-client/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
)

const (
	// SuccessSynced is used as part of the Event 'reason' when a Foo is synced
	successSynced = "Synced"
	failedSynced  = "FailedSync"
	// is synced successfully
	messageResourceSynced = "User synced successfully"
	controllerName        = "user-controller"
	// user finalizer
	finalizer       = "finalizers.kubesphere.io/users"
	syncFailMessage = "Failed to sync: %s"

	needSyncToLLdapAna = "iam.kubesphere.io/sync-to-lldap"
	syncedToLLdapAna   = "iam.kubesphere.io/synced-to-lldap"
	regularGroup       = "lldap_regular"

	globalAdmin = "iam.kubesphere.io/globalrole"
)

// Reconciler reconciles a User object
type Reconciler struct {
	client.Client
	LLdapClient             *lclient.Client
	KubeconfigClient        kubeconfig.Interface
	Logger                  logr.Logger
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Client == nil {
		r.Client = mgr.GetClient()
	}

	if r.Logger == nil {
		r.Logger = ctrl.Log.WithName("controllers").WithName(controllerName)
	}
	if r.Scheme == nil {
		r.Scheme = mgr.GetScheme()
	}
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor(controllerName)
	}
	if r.MaxConcurrentReconciles <= 0 {
		r.MaxConcurrentReconciles = 1
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named(controllerName).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.MaxConcurrentReconciles,
		}).
		For(&iamv1alpha2.User{}).
		Complete(r)
}

func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	logger := r.Logger.WithValues("user", req.NamespacedName)
	user := &iamv1alpha2.User{}
	err := r.Get(ctx, req.NamespacedName, user)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	klog.V(0).Infof("name=%s, user1: %v", user.Name, user.Spec.InitialPassword)
	if user.ObjectMeta.DeletionTimestamp.IsZero() {
		// The object is not being deleted, so if it does not have our finalizer,
		// then lets add the finalizer and update the object.
		if !sliceutil.HasString(user.Finalizers, finalizer) {
			user.ObjectMeta.Finalizers = append(user.ObjectMeta.Finalizers, finalizer)
			klog.V(0).Infof("name=%s,user2: %v", user.Name, user.Spec.InitialPassword)

			if err = r.Update(ctx, user, &client.UpdateOptions{}); err != nil {
				logger.Error(err, "failed to update user")
				return ctrl.Result{}, err
			}
		}
	} else {
		// The object is being deleted
		if sliceutil.HasString(user.ObjectMeta.Finalizers, finalizer) {

			if err = r.deleteRoleBindings(ctx, user); err != nil {
				r.Recorder.Event(user, corev1.EventTypeWarning, failedSynced, fmt.Sprintf(syncFailMessage, err))
				return ctrl.Result{}, err
			}

			// remove our finalizer from the list and update it.
			user.Finalizers = sliceutil.RemoveString(user.ObjectMeta.Finalizers, func(item string) bool {
				return item == finalizer
			})
			klog.V(0).Infof("name=%s,user3: %v", user.Name, user.Spec.InitialPassword)

			if err = r.Update(ctx, user, &client.UpdateOptions{}); err != nil {
				klog.Error(err)
				r.Recorder.Event(user, corev1.EventTypeWarning, failedSynced, fmt.Sprintf(syncFailMessage, err))
				return ctrl.Result{}, err
			}
		}

		// Our finalizer has finished, so the reconciler can do nothing.
		return ctrl.Result{}, err
	}
	if r.LLdapClient == nil {
		bindUsername, err := r.getCredentialVal(ctx, "lldap-ldap-user-dn")
		if err != nil {
			return ctrl.Result{}, err
		}
		bindPassword, err := r.getCredentialVal(ctx, "lldap-ldap-user-pass")
		if err != nil {
			return ctrl.Result{}, err
		}

		lldapClient, err := lclient.New(&lconfig.Config{
			Host:       "http://lldap-service.os-system:17170",
			Username:   bindUsername,
			Password:   bindPassword,
			TokenCache: memory.New(),
		})
		klog.V(0).Infof("bindUsername: %s,bindPassword: %s", bindUsername, bindPassword)
		if err != nil {
			return ctrl.Result{}, err
		}
		r.LLdapClient = lldapClient
	}

	// sync user with label "iam.kubesphere.io/sync-to-lldap": true and "iam.kubesphere.io/synced-to-lldap": false
	ana := user.Annotations

	isNeedSyncToLLDap, _ := strconv.ParseBool(ana[needSyncToLLdapAna])
	synced, _ := strconv.ParseBool(ana[syncedToLLdapAna])
	klog.V(0).Infof("isNeedSyncToLLDap: %v,synced: %v", isNeedSyncToLLDap, synced)
	if isNeedSyncToLLDap && !synced {
		klog.V(0).Infof("sync user from ks to lldap")
		var sync iamv1alpha2.Sync
		key := types.NamespacedName{Name: "lldap"}
		err = r.Get(ctx, key, &sync)
		klog.V(0).Infof("sync user to lldap: %v", err)
		if err == nil {
			u := generated.CreateUserInput{
				Id:          user.Name,
				Email:       user.Spec.Email,
				DisplayName: user.Name,
			}
			klog.V(0).Infof("create username:%s,password:%s", user.Name, user.Spec.InitialPassword)
			_, err = r.LLdapClient.Users().Create(ctx, &u, user.Spec.InitialPassword)
			if err != nil {
				klog.V(0).Infof("create lldap user err=%v", err)
				if lapierrors.IsAlreadyExists(err) {
					klog.V(0).Infof("", err)
				} else {
					return ctrl.Result{}, err
				}
			}
			//if ana[globalAdmin] == "true"
			lldapGroupID := 1
			if isAdmin, _ := strconv.ParseBool(ana[globalAdmin]); !isAdmin {
				g, err := r.LLdapClient.Groups().GetByName(ctx, regularGroup)
				if err != nil {
					// lldap_regular group will always exists ensure by lldap
					return ctrl.Result{}, err
				}
				lldapGroupID = g.Id

			}
			err = r.LLdapClient.Groups().AddUser(ctx, user.Name, lldapGroupID)
			if err != nil {
				klog.V(0).Infof("add user to group err=%v", err)
				if lapierrors.IsAlreadyExists(err) {
					klog.V(0).Infof("", err)
				} else {
					return ctrl.Result{}, err
				}
			}

			user.Annotations[syncedToLLdapAna] = "true"
			user.Labels = make(map[string]string)
			user.Labels["iam.kubesphere.io/user-provider"] = "lldap"
			user.Spec.InitialPassword = ""
			err = r.Update(ctx, user, &client.UpdateOptions{})
			if err != nil {
				klog.V(0).Infof("update user....: %v", err)
				return ctrl.Result{}, err
			}
			klog.V(0).Infof("successes to sync user %d to lldap", user.Name)
		}
	}

	if r.KubeconfigClient != nil {
		// ensure user KubeconfigClient configmap is created
		if err = r.KubeconfigClient.CreateKubeConfig(user); err != nil {
			klog.Error(err)
			r.Recorder.Event(user, corev1.EventTypeWarning, failedSynced, fmt.Sprintf(syncFailMessage, err))
			return ctrl.Result{}, err
		}
	}

	r.Recorder.Event(user, corev1.EventTypeNormal, successSynced, messageResourceSynced)

	return ctrl.Result{}, nil
}

func (r *Reconciler) ensureNotControlledByKubefed(ctx context.Context, user *iamv1alpha2.User) error {
	if user.Labels[constants.KubefedManagedLabel] != "false" {
		if user.Labels == nil {
			user.Labels = make(map[string]string, 0)
		}
		user.Labels[constants.KubefedManagedLabel] = "false"
		err := r.Update(ctx, user, &client.UpdateOptions{})
		if err != nil {
			klog.Error(err)
			return err
		}
	}
	return nil
}

func (r *Reconciler) deleteRoleBindings(ctx context.Context, user *iamv1alpha2.User) error {
	if len(user.Name) > validation.LabelValueMaxLength {
		// ignore invalid label value error
		return nil
	}

	clusterRoleBinding := &rbacv1.ClusterRoleBinding{}
	err := r.Client.DeleteAllOf(ctx, clusterRoleBinding, client.MatchingLabels{iamv1alpha2.UserReferenceLabel: user.Name})
	if err != nil {
		return err
	}

	roleBindingList := &rbacv1.RoleBindingList{}
	err = r.Client.List(ctx, roleBindingList, client.MatchingLabels{iamv1alpha2.UserReferenceLabel: user.Name})
	if err != nil {
		return err
	}

	for _, roleBinding := range roleBindingList.Items {
		err = r.Client.Delete(ctx, &roleBinding)
		if err != nil {
			return err
		}
	}
	return nil
}

// syncUserStatus Update the user status
func (r *Reconciler) syncUserStatus(ctx context.Context, user *iamv1alpha2.User) error {
	user.Status = iamv1alpha2.UserStatus{
		State:              iamv1alpha2.UserActive,
		LastTransitionTime: &metav1.Time{Time: time.Now()},
	}
	err := r.Update(ctx, user, &client.UpdateOptions{})
	if err != nil {
		return err
	}

	return nil
}

func (r *Reconciler) getCredentialVal(ctx context.Context, key string) (string, error) {
	var secret corev1.Secret
	k := types.NamespacedName{Name: "lldap-credentials", Namespace: "os-system"}
	err := r.Client.Get(ctx, k, &secret)
	if err != nil {
		return "", err
	}
	if value, ok := secret.Data[key]; ok {
		return string(value), nil
	}
	return "", fmt.Errorf("can not find credentialval for key %s", key)

}
