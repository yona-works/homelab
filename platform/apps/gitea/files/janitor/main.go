package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"os"
	"strings"

	"code.gitea.io/sdk/gitea"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const host = "git.yona.works"
const requestBase = host + "/request"
const annProcessedHash = host + "/processed-hash"

type SecretHandler interface {
	Start(client *kubernetes.Clientset, giteaClient *gitea.Client, stopCh <-chan struct{})
}

func main() {
	fmt.Println("Gitea janitor service started")

	k8sClient := getK8sClient()
	giteaClient := getGiteaClient()
	startWorkers(k8sClient, giteaClient)
}

func getK8sClient() *kubernetes.Clientset {
	kubeConfigPath := os.Getenv("KUBECONFIG")
	kubeconfig := flag.String("kubeconfig", kubeConfigPath, "absolute path to the kubeconfig file")
	flag.Parse()
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	k8sClient := kubernetes.NewForConfigOrDie(config)
	fmt.Println("Connected to Kubernetes cluster")
	return k8sClient
}

func getGiteaClient() *gitea.Client {
	giteaHost := os.Getenv("GITEA_HOST")
	giteaUser := os.Getenv("GITEA_USER")
	giteaPassword := os.Getenv("GITEA_PASSWORD")

	options := gitea.SetBasicAuth(giteaUser, giteaPassword)
	giteaClient, err := gitea.NewClient(giteaHost, options)
	if err != nil {
		panic(err.Error())
	}
	return giteaClient
}

func startWorkers(k8sClient *kubernetes.Clientset, giteaClient *gitea.Client) {
	fmt.Println("Starting workers")

	stopCh := make(chan struct{})
	defer close(stopCh)

	// List of handlers to start
	workerHandlers := []SecretHandler{
		&TokenRequestHandler{},
		&Oauth2AppRequestHandler{},
	}

	for _, h := range workerHandlers {
		go h.Start(k8sClient, giteaClient, stopCh)
	}

	<-stopCh
}

func hash(name ...string) string {
	data := strings.Join(name, ":")
	hash := sha256.Sum256([]byte(data))
	hexHash := fmt.Sprintf("%x", hash)
	return hexHash[:63]
}

// ListAll traverses all pages of a Gitea resource and returns a single slice.
func ListAll[T any](fetcher func(gitea.ListOptions) ([]T, *gitea.Response, error)) ([]T, error) {
	var allResults []T
	opts := gitea.ListOptions{
		Page:     1,
		PageSize: 50, // Gitea's default max is often 50 or 100
	}

	for {
		results, resp, err := fetcher(opts)
		if err != nil {
			return nil, err
		}

		allResults = append(allResults, results...)

		// If there are no more pages, or we received fewer items than requested, we're done
		if resp.NextPage == 0 {
			break
		}
		opts.Page = resp.NextPage
	}

	return allResults, nil
}
