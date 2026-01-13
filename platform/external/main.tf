module "extra_secrets" {
  source = "./modules/extra-secrets"
  data   = var.extra_secrets
}
