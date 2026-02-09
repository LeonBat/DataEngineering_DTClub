variable "credentials" {
  description = "My Credentials"
  default     = "./keys/my_key.json"
}


variable "project" {
  description = "Project "
  default     = "natural-oath-484714-i1"
}

variable "region" {
  description = "Project Region"
  default     = "europe-west6-c"
}

variable "location" {
  description = "Project Location"
  default     = "EU"
}


variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}


variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "natural-oath-484714-i1-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
