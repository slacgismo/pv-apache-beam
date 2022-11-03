#!/bin/bash
echo "## Run pipeline with '--setup_file'."
PROJECT=$(gcloud config get-value project)
REGION=us-east1
export PYTHON_VERSION=$(python -c "import sys; print('{}.{}'.format(*sys.version_info[0:2]))")

# Build SDK container image
echo "## Build SDK container."

IMAGE_URI="gcr.io/$PROJECT/pv_batch_python${PYTHON_VERSION}:2.42.0"
gcloud builds submit . --tag $IMAGE_URI



# Install apache-beam
echo "## Install custom packages"
pip install ./transformers

# Run pipeline

# --------------------
# Local
# --------------------
echo "## Run local container"
python -m main \
  --runner PortableRunner \
  --input gs://jimmy_beam_bucket/pvdata/10010.csv gs://jimmy_beam_bucket/pvdata/10059.csv gs://jimmy_beam_bucket/pvdata/10284.csv \
  --output gs://jimmy_beam_bucket/demo_files/results \
  --platform linux/amd64 \
  --job_endpoint embed \
  --environment_type "DOCKER"  \
  --experiments no_use_multiple_sdk_containers \
  --environment_config $IMAGE_URI





