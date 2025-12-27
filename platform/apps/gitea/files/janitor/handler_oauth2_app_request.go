package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"code.gitea.io/sdk/gitea"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

/*
Oauth2AppRequestHandler monitors kubernetes secrets, for secrets with label git.yona.works/request=oauth2-app.
If a secret with said label changes, this handler will create a OAuth2 app in the Gitea administrator account
and updates the secret with the client id and secret. This way, another application can request client id and secret,
without having to give the admin credentials to the app for its deployment

The secret must have the following annotations:
  - git.yona.works/request-oauth2-app-name: the name of the app
  - git.yona.works/request-oauth2-app-redirect-url: the redirect url for the app
  - git.yona.works/request-oauth2-app-client-id-data-key: the key to store the client id in the secret's data field
  - git.yona.works/request-oauth2-app-client-secret-data-key: the key to store the client secret in the secret's data field

If a client with the given name, but a different redirect url already exists, it will be deleted and replaced with a new one.
This handler stores a hash with the secret (annotation git.yona.works/processed-hash) in order to determine if the secret
needs to be processed again. The secret can be rotated by deleting this annotation.
*/
type Oauth2AppRequestHandler struct{}

const lblRequestOauth2App = requestBase + "=oauth2-app"
const appRequestBase = requestBase + "-oauth2-app"
const annAppName = appRequestBase + "-name"
const annAppRedirectUrl = appRequestBase + "-redirect-url"
const annAppClientIdKey = appRequestBase + "-client-id-data-key"
const annAppClientSecretKey = appRequestBase + "-client-secret-data-key"

func (h *Oauth2AppRequestHandler) Start(k8sClient *kubernetes.Clientset, giteaClient *gitea.Client, stopCh <-chan struct{}) {
	fmt.Println("Starting OAuth2 App request worker...")

	ch := make(chan *corev1.Secret, 100)

	// Worker logic
	go func() {
		for {
			select {
			case secret := <-ch:
				if err := h.handle(k8sClient, giteaClient, secret); err != nil {
					log.Printf("Error handling oauth2 app request: %v", err)
				}
			case <-stopCh:
				return
			}
		}
	}()

	// Informer logic
	factory := informers.NewSharedInformerFactoryWithOptions(k8sClient, time.Minute*10,
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = lblRequestOauth2App
		}),
	)

	informer := factory.Core().V1().Secrets().Informer()

	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			secret := obj.(*corev1.Secret)
			h.dispatch(secret, ch)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			newSecret := newObj.(*corev1.Secret)
			h.dispatch(newSecret, ch)
		},
	})
	if err != nil {
		log.Fatalf("Failed to add event handler for TokenRequestHandler: %v", err)
	}

	factory.Start(stopCh)
	if !cache.WaitForCacheSync(stopCh, informer.HasSynced) {
		log.Fatal("Timed out waiting for caches to sync (TokenRequestHandler)")
	}
}

func (h *Oauth2AppRequestHandler) handle(k8sClient *kubernetes.Clientset, giteaClient *gitea.Client, secret *corev1.Secret) error {
	fmt.Printf("Handling Oauth2 App request for secret: %s/%s\n", secret.Namespace, secret.Name)

	name := secret.Annotations[annAppName]
	if name == "" {
		return fmt.Errorf("missing required annotation '%s'", annAppName)
	}

	redirectUrl := secret.Annotations[annAppRedirectUrl]
	if redirectUrl == "" {
		return fmt.Errorf("missing required annotation '%s'", annAppRedirectUrl)
	}

	clientIdDataKey := secret.Annotations[annAppClientIdKey]
	if clientIdDataKey == "" {
		clientIdDataKey = "clientId"
	}

	clientSecretDataKey := secret.Annotations[annAppClientSecretKey]
	if clientSecretDataKey == "" {
		clientSecretDataKey = "clientSecret"
	}

	hash := hash(name, redirectUrl, clientIdDataKey, clientSecretDataKey)

	if secret.Annotations[annProcessedHash] == fmt.Sprintf("%x", hash) {
		fmt.Printf("OAuth2 App request for secret %s/%s is already processed, skipping\n", secret.Namespace, secret.Name)
		return nil
	}

	fmt.Printf("Storing client id on in secret in '$.Data.%s'\n", clientIdDataKey)
	fmt.Printf("Storing client secret on in secret in '$.Data.%s'\n", clientSecretDataKey)

	fmt.Printf("Checking for existing OAuth2 app name %s\n", name)

	apps, err := ListAll(func(opts gitea.ListOptions) ([]*gitea.Oauth2, *gitea.Response, error) {
		return giteaClient.ListOauth2(
			gitea.ListOauth2Option{ListOptions: opts})
	})

	for _, app := range apps {
		if app.Name == name {
			fmt.Printf("App with name %s already exists, recreating with desired properties\n", name)
			_, err := giteaClient.DeleteOauth2(app.ID)
			if err != nil {
				return err
			}
		}
	}

	fmt.Printf("Creating Oauth2 app with name %s and redirect-url %s\n", name, redirectUrl)

	app, _, err := giteaClient.CreateOauth2(gitea.CreateOauth2Option{
		Name:               name,
		ConfidentialClient: true,
		RedirectURIs:       []string{redirectUrl},
	})
	if err != nil {
		return err
	}

	fmt.Printf("Oauth2 app %s created with clientId: %s\n", app.Name, app.ClientID)
	updatedSecret := secret.DeepCopy()

	if updatedSecret.Data == nil {
		updatedSecret.Data = make(map[string][]byte)
	}
	// Update secret with the tclient id and secret
	updatedSecret.Data[clientIdDataKey] = []byte(app.ClientID)
	updatedSecret.Data[clientSecretDataKey] = []byte(app.ClientSecret)
	// Add hash
	updatedSecret.Annotations[annProcessedHash] = fmt.Sprintf("%x", hash)

	_, err = k8sClient.CoreV1().Secrets(updatedSecret.Namespace).Update(context.Background(), updatedSecret, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	fmt.Printf("Updated secret: %s/%s\n", updatedSecret.Namespace, updatedSecret.Name)
	return nil
}

func (h *Oauth2AppRequestHandler) dispatch(secret *corev1.Secret, ch chan<- *corev1.Secret) {
	select {
	case ch <- secret:
	default:
		log.Printf("Oauth2AppRequest channel is full, dropping event for %s/%s", secret.Namespace, secret.Name)
	}
}
