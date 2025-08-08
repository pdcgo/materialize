provider "google" {
  project = ""
  region  = "asia-southeast2"
  zone    = "asia-southeast2-a"
}


resource "google_compute_instance" "materialize_vm" {
  name         = "materialize-vm"
  machine_type = "e2-medium"  # Small but bigger than e2-micro for stability
  zone         = "asia-southeast2-a"

  tags = ["materialize-test"]

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = 15
    }
  }

  network_interface {
    network = "default"
    access_config {}
  } 
  allow_stopping_for_update = true 
  metadata_startup_script = file("startup.sh")
}

resource "google_compute_firewall" "allow_materialize_port" {
  name    = "allow-materialize-port"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["6874", "6875", "6876"]
  }
  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["materialize-test"]
}