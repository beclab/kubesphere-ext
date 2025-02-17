package user

import (
	"context"
	"fmt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	iamv1alpha2 "kubesphere.io/api/iam/v1alpha2"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	"kubesphere.io/kubesphere/pkg/simple/client/lldap"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"time"
)

const userSyncProviderKey = "iam.kubesphere.io/user-provider"

type SyncReconciler struct {
	client.Client
	*kubernetes.Clientset
	*lldap.LLdapOperator
	Scheme                  *runtime.Scheme
	MaxConcurrentReconciles int
}

func (r *SyncReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)
	ctrl.Log.Info("user-sync-from-lldap reconcile request", "name", req.Name, "namespace", req.Namespace)

	obj := &iamv1alpha2.Sync{}
	err := r.Get(ctx, req.NamespacedName, obj)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if r.LLdapOperator == nil {
		op, err := lldap.New(obj.Spec.LLdap)
		if err != nil {
			return ctrl.Result{}, err
		}
		r.LLdapOperator = op
	}

	r.UserBlacklist = obj.Spec.LLdap.UserBlacklist
	r.GroupWhitelist = obj.Spec.LLdap.GroupWhitelist

	errs := make([]error, 0)
	users, err := r.LLdapOperator.GetUserList()

	if err != nil {
		return ctrl.Result{}, err
	}
	fmt.Println("users:", users)

	for _, u := range users {
		userObj := &iamv1alpha2.User{}
		err = r.Get(ctx, types.NamespacedName{Name: u.Id, Namespace: ""}, userObj)
		// create user in crd if user exist in lldap
		if apierrors.IsNotFound(err) {
			userObj = &iamv1alpha2.User{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Sync",
					APIVersion: iamv1alpha2.SchemeGroupVersion.String(),
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: u.Id,
				},
				Spec: iamv1alpha2.UserSpec{
					Email:  u.Email,
					Groups: u.Groups,
				},
				Status: iamv1alpha2.UserStatus{
					State: iamv1alpha2.UserActive,
				},
			}
		} else if err != nil {
			errs = append(errs, err)
			continue
		}
		userObj.SetLabels(map[string]string{userSyncProviderKey: "lldap"})

		err = r.CreateOrUpdateResource(ctx, "", userObj)
		if err != nil {
			errs = append(errs, err)
			continue
		}

	}
	err = r.pruneUsers(ctx, users, "lldap")
	if err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return ctrl.Result{}, utilerrors.NewAggregate(errs)
	}

	return ctrl.Result{
		RequeueAfter: time.Second * 2,
	}, nil
}

func (r *SyncReconciler) pruneUsers(ctx context.Context, syncUsers []lldap.User, providerName string) error {
	// prune user
	opts := []client.ListOption{
		client.InNamespace(""),
		client.MatchingLabels{userSyncProviderKey: providerName},
	}
	userList := &iamv1alpha2.UserList{}
	err := r.List(ctx, userList, opts...)
	if err != nil {
		return err
	}
	// user list in crd
	userInCrd := make([]string, 0)

	for _, u := range userList.Items {
		userInCrd = append(userInCrd, u.Name)
	}
	klog.Infof("userList: %#v", userInCrd)
	klog.Infof("syncUsers: %v", syncUsers)

	for _, u := range userList.Items {
		userFound := r.isUserFound(u, syncUsers)
		klog.Infof("delete user: %v", userFound)
		if !userFound {
			err = r.Delete(ctx, &u)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *SyncReconciler) isUserFound(targetUser iamv1alpha2.User, users []lldap.User) bool {
	for _, u := range users {
		if targetUser.Name == u.Id {
			return true
		}
	}
	return false
}

func (r *SyncReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Client == nil {
		r.Client = mgr.GetClient()
	}
	if r.Scheme == nil {
		r.Scheme = mgr.GetScheme()
	}
	if r.MaxConcurrentReconciles <= 0 {
		r.MaxConcurrentReconciles = 1
	}

	err := ctrl.NewControllerManagedBy(mgr).
		Named("user-sync-from-lldap-controller").
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.MaxConcurrentReconciles,
		}).
		For(&iamv1alpha2.Sync{}).Complete(r)
	if err != nil {
		return err
	}
	return nil
}

func (r *SyncReconciler) CreateOrUpdateResource(ctx context.Context, namespace string, obj client.Object) error {
	if namespace != "" {
		obj.SetNamespace(namespace)
	}
	obj2 := &unstructured.Unstructured{}
	obj2.SetGroupVersionKind(obj.GetObjectKind().GroupVersionKind())

	err := r.Get(ctx, types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}, obj2)

	if apierrors.IsNotFound(err) {
		err = r.Create(ctx, obj)
		if err != nil {
			return err
		}
		return nil
	}
	if err == nil {
		obj.SetResourceVersion(obj2.GetResourceVersion())
		err = r.Update(ctx, obj)
		if err != nil {
			return err
		}
		return nil
	}
	return err
}

func (r *SyncReconciler) GetUser(ctx context.Context, name string) (*lldap.User, error) {
	user, err := r.LLdapClient.Users().Get(ctx, name)
	if err != nil {
		return nil, err
	}
	groups := make([]string, 0)
	for _, g := range user.Groups {
		groups = append(groups, g.DisplayName)
	}
	u := &lldap.User{
		Id:          user.Id,
		DisplayName: user.DisplayName,
		Groups:      groups,
	}
	return u, nil
}
