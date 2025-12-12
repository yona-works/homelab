.POSIX:
.PHONY: plan apply
.EXPORT_ALL_VARIABLES:

KUBECONFIG = $(shell pwd)/../../metal/kubeconfig.yaml
KUBE_CONFIG_PATH = $(KUBECONFIG)

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
	tofu plan

apply: .terraform.lock.hcl
	tofu apply -auto-approve
