#!/bin/bash
echo "## Run pipeline with '--setup_file'."
PROJECT=$(gcloud config get-value project)
REGION=us-east1
export PYTHON_VERSION=$(python -c "import sys; print('{}.{}'.format(*sys.version_info[0:2]))")

# Build SDK container image
echo "## Build SDK container."

IMAGE_URI="gcr.io/$PROJECT/gridlabd_python_${PYTHON_VERSION}:2.42.0"
gcloud builds submit . --tag $IMAGE_URI



# Install apache-beam
echo "## Install custom packages"
pip install ./transformers

# Run pipeline

# --------------------
# Local GCD
# --------------------
echo "## Run local container"
python -m main \
  --runner PortableRunner \
  --input ./kinglear-1.txt \
  --output gs://jimmy_beam_bucket/demo_files/results \
  --platform linux/amd64 \
  --job_endpoint embed \
  --environment_type "DOCKER"  \
  --environment_config $IMAGE_URI


IMAGE_LOCAL="jimmyleu76/beam_test:latest"
docker build -t $IMAGE_LOCAL .
docker push $IMAGE_LOCAL
# --------------------
# Local Image
# --------------------
echo "## Run local container"
python -m main \
  --runner PortableRunner \
  --input ./kinglear-1.txt \
  --output gs://jimmy_beam_bucket/demo_files/results \
  --platform linux/amd64 \
  --job_endpoint embed \
  --environment_type "DOCKER"  \
  --environment_config $IMAGE_LOCAL
