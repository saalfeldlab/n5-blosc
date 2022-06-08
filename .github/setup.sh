#!/bin/sh
curl -fsLO https://raw.githubusercontent.com/scijava/scijava-scripts/main/ci-setup-github-actions.sh
sh ci-setup-github-actions.sh

# Install Blosc native library.
sudo apt-get update
sudo apt-get install -y libblosc1
