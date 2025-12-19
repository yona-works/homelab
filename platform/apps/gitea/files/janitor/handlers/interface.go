package handlers

import (
	"code.gitea.io/sdk/gitea"
	"k8s.io/client-go/kubernetes"
)

type SecretHandler interface {
	Start(client *kubernetes.Clientset, giteaClient *gitea.Client, stopCh <-chan struct{})
}
