package main

import (
	"flag"
	"fmt"
	"os"

	"code.gitea.io/sdk/gitea"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

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
	}

	for _, h := range workerHandlers {
		go h.Start(k8sClient, giteaClient, stopCh)
	}

	<-stopCh
}
