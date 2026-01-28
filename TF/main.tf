resource "google_storage_bucket" "landing" {
  name          = "bucket-de-pau-para-spotify"
  location      = "europe-southwest1"
  force_destroy = false
}
resource "google_storage_bucket" "beam" {
  name          = "bucket-de-pau-para-beam"
  location      = "europe-southwest1"
  force_destroy = false
}

resource "google_firestore_database" "database" {
  project     = var.project_id
  name        = "(default)"
  location_id = "europe-southwest1"
  type        = "FIRESTORE_NATIVE"
}

resource "google_bigquery_dataset" "bq_dataset" {
  dataset_id  = "edem_data"
  project     = var.project_id
  location    = "europe-west1"
}