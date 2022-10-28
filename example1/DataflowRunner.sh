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



# # --------------------
# # GCD
# # --------------------
echo "## Run on google cloud platform"
python -m main --runner DataflowRunner \
  --project $PROJECT \
  --region $REGION \
  --job_name example13 \
  --experiments use_runner_v2 \
  --input gs://jimmy_beam_bucket/pvdata/10010.csv gs://jimmy_beam_bucket/pvdata/10059.csv gs://jimmy_beam_bucket/pvdata/10284.csv \
  --output gs://jimmy_beam_bucket/demo_files/part \
  --temp_location gs://jimmy_beam_bucket/demo_files/temp \
  --experiments no_use_multiple_sdk_containers \
  --sdk_container_image $IMAGE_URI \
  --sdk_location container \
  --autoscalingAlgorithm NONE \
  --numWorkers 3



