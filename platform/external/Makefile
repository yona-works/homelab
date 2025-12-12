.POSIX:
.PHONY: plan apply

export KUBECONFIG = $(shell pwd)/../../metal/kubeconfig.yaml
export KUBE_CONFIG_PATH = $(KUBECONFIG)

debug:
	echo KUBECONFIG: $(KUBECONFIG)
	head -n 1 $(KUBECONFIG)
	echo KUBE_CONFIG_PATH: $(KUBE_CONFIG_PATH)
	head -n 1 $(KUBE_CONFIG_PATH)

default: apply

~/.terraform.d/credentials.tfrc.json:
	tofu login

terraform.tfvars:
	cp terraform.tfvars.example ${@}
	nvim ${@}

.terraform.lock.hcl: ~/.terraform.d/credentials.tfrc.json versions.tf terraform.tfvars
	tofu init
	touch ${@}

plan: .terraform.lock.hcl
	env | grep -E 'KUBECONFIG|KUBE_CONFIG_PATH' || true
	tofu plan

apply: .terraform.lock.hcl
	env | grep -E 'KUBECONFIG|KUBE_CONFIG_PATH' || true
	tofu apply -auto-approve
