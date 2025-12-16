.POSIX:

export KUBECONFIG = $(shell pwd)/../metal/kubeconfig.yaml

.PHONY: *

default: bootstrap external

bootstrap:
	ansible-playbook \
		bootstrap.yml

external:
	make -C external


