variable "cloudflare" {
  type = object({
    email      = string
    api_key    = string
    account_id = string
  })

  sensitive = true
}