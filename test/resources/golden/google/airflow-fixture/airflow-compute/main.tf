terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.0"
    }
  }
}

provider "google" {
  project = "airflow-example"
  region  = "europe-west4"
  zone    = "europe-west4-b"
}

resource "google_compute_network" "network" {
  name                    = "airflow"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "public" {
  name          = "airflow"
  region        = "europe-west4"
  network       = google_compute_network.network.id
  ip_cidr_range = "10.20.1.0/24"
}

resource "google_compute_firewall" "node1" {
  name    = "airflow"
  network = google_compute_network.network.name

  allow {
    protocol = "tcp"
    ports    = ["22", "80", "443"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["airflow"]
}

resource "google_compute_address" "node1" {
  name   = "airflow"
  region = "europe-west4"
}

resource "google_compute_instance" "node1" {
  name         = "airflow"
  machine_type = "t2a-standard-1"
  zone         = "europe-west4-b"
  tags         = ["airflow"]

  boot_disk {
    initialize_params {
      image = "projects/ubuntu-os-cloud/global/images/ubuntu-example"
      size  = 30
      type  = "pd-balanced"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.public.id
    access_config {
      nat_ip = google_compute_address.node1.address
    }
  }

  metadata = {
    ssh-keys = "ubuntu:${trimspace(file("/tmp/airflow.pub"))}"
  }

  connection {
    type  = "ssh"
    user  = "ubuntu"
    agent = true
    host  = google_compute_address.node1.address
  }
  provisioner "remote-exec" {
    inline = ["ls"]
  }
  lifecycle {
    prevent_destroy = true
  }
}

output "params" {
  value = {
    ip     = google_compute_address.node1.address
    sudoer = "ubuntu"
    uid    = "1000"
    name   = "airflow-fixture"
    user   = "ubuntu"
  }
}
