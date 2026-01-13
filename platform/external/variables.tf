variable "extra_secrets" {
  type        = map(string)
  description = "Key-value pairs of extra secrets that cannot be randomly generated (e.g. third party API tokens)"
  sensitive   = true
  default     = {}
}
