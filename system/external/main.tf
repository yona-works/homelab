module "ntfy" {
  source = "./modules/ntfy"
  auth   = var.ntfy
}

module "cloudflare" {
  source     = "./modules/cloudflare"
  cloudflare = var.cloudflare
}
