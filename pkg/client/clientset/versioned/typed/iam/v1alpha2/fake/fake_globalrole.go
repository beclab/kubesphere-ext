/*
Copyright 2020 The KubeSphere Authors.

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

// Code generated by client-gen. DO NOT EDIT.

package fake

import (
	"context"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
	v1alpha2 "kubesphere.io/api/iam/v1alpha2"
)

// FakeGlobalRoles implements GlobalRoleInterface
type FakeGlobalRoles struct {
	Fake *FakeIamV1alpha2
}

var globalrolesResource = schema.GroupVersionResource{Group: "iam.bytetrade.io", Version: "v1alpha2", Resource: "globalroles"}

var globalrolesKind = schema.GroupVersionKind{Group: "iam.bytetrade.io", Version: "v1alpha2", Kind: "GlobalRole"}

// Get takes name of the globalRole, and returns the corresponding globalRole object, and an error if there is any.
func (c *FakeGlobalRoles) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1alpha2.GlobalRole, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootGetAction(globalrolesResource, name), &v1alpha2.GlobalRole{})
	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha2.GlobalRole), err
}

// List takes label and field selectors, and returns the list of GlobalRoles that match those selectors.
func (c *FakeGlobalRoles) List(ctx context.Context, opts v1.ListOptions) (result *v1alpha2.GlobalRoleList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootListAction(globalrolesResource, globalrolesKind, opts), &v1alpha2.GlobalRoleList{})
	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v1alpha2.GlobalRoleList{ListMeta: obj.(*v1alpha2.GlobalRoleList).ListMeta}
	for _, item := range obj.(*v1alpha2.GlobalRoleList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested globalRoles.
func (c *FakeGlobalRoles) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewRootWatchAction(globalrolesResource, opts))
}

// Create takes the representation of a globalRole and creates it.  Returns the server's representation of the globalRole, and an error, if there is any.
func (c *FakeGlobalRoles) Create(ctx context.Context, globalRole *v1alpha2.GlobalRole, opts v1.CreateOptions) (result *v1alpha2.GlobalRole, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootCreateAction(globalrolesResource, globalRole), &v1alpha2.GlobalRole{})
	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha2.GlobalRole), err
}

// Update takes the representation of a globalRole and updates it. Returns the server's representation of the globalRole, and an error, if there is any.
func (c *FakeGlobalRoles) Update(ctx context.Context, globalRole *v1alpha2.GlobalRole, opts v1.UpdateOptions) (result *v1alpha2.GlobalRole, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootUpdateAction(globalrolesResource, globalRole), &v1alpha2.GlobalRole{})
	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha2.GlobalRole), err
}

// Delete takes name of the globalRole and deletes it. Returns an error if one occurs.
func (c *FakeGlobalRoles) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewRootDeleteAction(globalrolesResource, name), &v1alpha2.GlobalRole{})
	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeGlobalRoles) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	action := testing.NewRootDeleteCollectionAction(globalrolesResource, listOpts)

	_, err := c.Fake.Invokes(action, &v1alpha2.GlobalRoleList{})
	return err
}

// Patch applies the patch and returns the patched globalRole.
func (c *FakeGlobalRoles) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha2.GlobalRole, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewRootPatchSubresourceAction(globalrolesResource, name, pt, data, subresources...), &v1alpha2.GlobalRole{})
	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha2.GlobalRole), err
}
