package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"strings"
	"time"

	"code.gitea.io/sdk/gitea"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

/*
TokenRequestHandler monitors kubernetes secrets, for secrets with label git.yona.works/request=access-token.
If a secret with said label changes, this handler will create a token in the Gitea administrator account
and updates the secret with the token value. This way, another application can request a token with access,
without having to give the admin credentials to the app for its deployment

The secret must have the following annotations:
  - git.yona.works/request-access-token-name: the name of the token to create
  - git.yona.works/request-access-token-scopes: a comma-separated list of scopes to request.
    See: https://docs.gitea.com/next/development/api-usage#token-api for documentation on token scopes.

The token value will be stored in the "token" key of the secret's data field.

If a token with the given name, but different scopes already exists, it will be deleted and replaced with a new one.
This handler stores a hash with the secret (annotation git.yona.works/processed-hash) in order to determine if the secret
needs to be processed again. The secret can be rotated by deleting this annotation.
*/
type TokenRequestHandler struct{}

const lblRequestAccessToken = "git.yona.works/request=access-token"
const annTokenName = "git.yona.works/request-access-token-name"
const annTokenScopes = "git.yona.works/request-access-token-scopes"
const annSecretDataKey = "git.yona.works/request-access-token-secret-data-key"
const annProcessedHash = "git.yona.works/processed-hash"

func (h *TokenRequestHandler) Start(k8sClient *kubernetes.Clientset, giteaClient *gitea.Client, stopCh <-chan struct{}) {
	fmt.Println("Starting Token Request Worker...")

	ch := make(chan *corev1.Secret, 100)

	// Worker logic
	go func() {
		for {
			select {
			case secret := <-ch:
				if err := h.handle(k8sClient, giteaClient, secret); err != nil {
					log.Printf("Error handling access-token: %v", err)
				}
			case <-stopCh:
				return
			}
		}
	}()

	// Informer logic
	factory := informers.NewSharedInformerFactoryWithOptions(k8sClient, time.Minute*10,
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = lblRequestAccessToken
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

func (h *TokenRequestHandler) handle(k8sClient *kubernetes.Clientset, giteaClient *gitea.Client, secret *corev1.Secret) error {
	fmt.Printf("Handling Token Request for secret: %s/%s\n", secret.Namespace, secret.Name)

	// git.yona.works/request/access-token/name=<string>
	// git.yona.works/request/access-token/permissions=scope[,...]

	name := secret.Annotations[annTokenName]
	// name should be a valid identifier, e.g., alphanumeric with hyphens
	if name == "" {
		return fmt.Errorf("missing required annotation 'git.yona.works/request-access-token-name'")
	}

	scopesStr := secret.Annotations[annTokenScopes]
	// permissions should be present
	if scopesStr == "" {
		return fmt.Errorf("missing required annotation 'git.yona.works/request-access-token-scopes'")
	}
	// split permissions into a map of routes to read/write permissions
	scopes := parseScopes(scopesStr)

	hash := h.hash(name, scopesStr)

	if secret.Annotations[annProcessedHash] == fmt.Sprintf("%x", hash) {
		fmt.Printf("Token request for secret %s/%s is already processed, skipping\n", secret.Namespace, secret.Name)
		return nil
	}

	secretDataKey := secret.Annotations[annSecretDataKey]
	if secretDataKey == "" {
		secretDataKey = "token"
	}

	fmt.Printf("Storing token value on in secret in '$.Data.%s'", secretDataKey)

	fmt.Printf("Checking for existing token with name %s\n", name)
	tokens, _, err := giteaClient.ListAccessTokens(gitea.ListAccessTokensOptions{})
	for _, token := range tokens {
		if token.Name == name {
			fmt.Printf("Token %s already exists, recreating with desired permissions\n", name)
			_, err := giteaClient.DeleteAccessToken(token.ID)
			if err != nil {
				return err
			}
		}
	}

	fmt.Printf("Creating token with name %s\n", name)
	token, _, err := giteaClient.CreateAccessToken(gitea.CreateAccessTokenOption{
		Name:   name,
		Scopes: scopes,
	})
	if err != nil {
		return err
	}

	fmt.Printf("Token %s created with scopes: %v\n", token.Name, token.Scopes)

	// Update secret with the token value
	secret.Data[secretDataKey] = []byte(token.Token)
	// Add hash
	secret.Annotations[annProcessedHash] = fmt.Sprintf("%x", hash)

	_, err = k8sClient.CoreV1().Secrets(secret.Namespace).Update(context.Background(), secret, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	fmt.Printf("Updated secret: %s/%s\n", secret.Namespace, secret.Name)
	return nil
}

func (h *TokenRequestHandler) hash(name string, scopesStr string) string {
	data := fmt.Sprintf("%s:%s", name, scopesStr)
	hash := sha256.Sum256([]byte(data))
	hexHash := fmt.Sprintf("%x", hash)
	return hexHash[:63]
}

func parseScopes(permissions string) []gitea.AccessTokenScope {
	scopes := make([]gitea.AccessTokenScope, 0)

	// The permissions string is a semicolon-separated list of scopes
	permissionsArr := strings.Split(permissions, ",")

	// for loop over strings, splitting each entry and storing it in stringMap
	for _, entry := range permissionsArr {
		// Find corresponding gitea.AccessTokenScope enum value
		scope := gitea.AccessTokenScope(entry)
		scopes = append(scopes, scope)
	}
	return scopes
}

func (h *TokenRequestHandler) dispatch(secret *corev1.Secret, ch chan<- *corev1.Secret) {
	select {
	case ch <- secret:
	default:
		log.Printf("TokenRequest channel is full, dropping event for %s/%s", secret.Namespace, secret.Name)
	}
}
