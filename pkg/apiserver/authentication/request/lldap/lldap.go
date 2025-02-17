package lldap

import (
	"errors"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/klog"
	"net/http"
	"strings"
)

type Authenticator struct {
	auth authenticator.Token
}

func New(auth authenticator.Token) *Authenticator {
	return &Authenticator{auth: auth}
}

var invalidToken = errors.New("invalid jwt token")

func (a *Authenticator) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
	tokenString := strings.TrimSpace(req.Header.Get("Authorization"))
	klog.V(0).Infof("token in lldap AuthenticateRequest: %s", tokenString)
	if tokenString == "" {
		return nil, false, nil
	}

	resp, ok, err := a.auth.AuthenticateToken(req.Context(), tokenString)
	klog.V(0).Infof("lldap Authenticate ok:%v,err:%v", ok, err)
	klog.V(0).Infof("lldap Authenticate resp.user:%#v, resp.au", resp.User, resp.Audiences)
	if !ok && err == nil {
		err = invalidToken
	}
	return resp, ok, err
}
