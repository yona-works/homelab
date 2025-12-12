.POSIX:

export KUBECONFIG = $(shell pwd)/../metal/kubeconfig.yaml

.PHONY: *

default: external bootstrap

external:
	make -C external

bootstrap:
	ansible-playbook \
		bootstrap.yml


