
variable "aiven_pg_service_name" {
  type = string
}

resource "aiven_pg" "ps1" {
  project      = data.aiven_project.langchain_demo.project
  cloud_name   = "google-us-east1"
  plan         = "startup-4"
  service_name = var.aiven_pg_service_name

}

output "pg_service_endpoint_uri" {
  value     = aiven_pg.ps1.service_uri
  sensitive = true
}
