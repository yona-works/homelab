{
  description = "Homelab";

 inputs = {
    #nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.05";
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-25.05-darwin";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        notestKanidm = pkgs.kanidm.overrideAttrs (_: {
            doCheck = false;
        });
      in
      with pkgs;
      {
        devShells.default = mkShell {
          packages = [
            ansible
            ansible-lint
            bmake
            diffutils
            docker
            docker-compose
            dyff
            git
            glibcLocales
            go
            gotestsum
#            iproute2
            iproute2mac
            jq
            k9s
            notestKanidm
            kube3d
            kubectl
            kubernetes-helm
            kustomize
            libisoburn
            neovim
            openssh
            opentofu # Drop-in replacement for Terraform
            p7zip
            pre-commit
            qrencode
            shellcheck
            wireguard-tools
            yamllint

            (python3.withPackages (p: with p; [
              jinja2
              kubernetes
              mkdocs-material
              netaddr
              pexpect
              rich
            ]))
          ];
        };
      }
    );
}
