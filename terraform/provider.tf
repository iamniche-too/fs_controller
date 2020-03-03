provider "google" {
  credentials = file("~/.gcp/kafka-k8s-trial-4287e941a38f.json")
  project = "kafka-k8s-trial"
  region  = "europe-west2"
  zone    = "europe-west2-a"
}
