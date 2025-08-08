#!/bin/bash
exec > /var/log/startup-script.log 2>&1
set -eux

# Install Docker
apt-get update
apt-get install -y docker.io
systemctl enable docker
systemctl start docker

# Pull and run Materialize
docker run -d --name materialize -p 6876:6876 -p 6875:6875 -p 6874:6874 materialize/materialized:latest

