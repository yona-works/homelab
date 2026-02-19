.POSIX:

export KUBECONFIG = $(shell pwd)/../metal/kubeconfig.yaml

.PHONY: *

default: bootstrap

bootstrap:
	ansible-playbook \
		bootstrap.yml


