#!/bin/bash
echo "## Run pipeline with '--setup_file'."
PROJECT=$(gcloud config get-value project)
REGION=us-east1
export PYTHON_VERSION="3.9.1"

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
# echo "## Run local container"
# python -m main \
#   --runner PortableRunner \
#   --input ./kinglear-1.txt \
#   --output gs://jimmy_beam_bucket/demo_files/results \
#   --platform linux/amd64 \
#   --job_endpoint embed \
#   --environment_type "DOCKER"  \
#   --environment_config $IMAGE_URI


IMAGE_LOCAL="jimmyleu76/gridlabd_86_3.9.6:latest"
docker build -t gridlabd .
docker tag gridlabd $IMAGE_LOCAL
docker push $IMAGE_LOCAL
# --------------------
# Local Image
# --------------------
echo "## Run local container"
python -m main \
  --runner PortableRunner \
  --input gs://jimmy_beam_bucket/PGE/AT0001.glm \
  --output  gs://jimmy_beam_bucket/demo_files/gridlabd \
  --job_endpoint embed \
  --environment_type "DOCKER"  \
  --environment_config $IMAGE_LOCAL

  python -m main \
  --runner PortableRunner \
  --input ./AL0001.glm  \
  --output  gs://jimmy_beam_bucket/demo_files/g_result \
  --job_endpoint embed \
  --environment_type "DOCKER"  \
  --environment_config $IMAGE_LOCAL


python -m main --runner DataflowRunner \
  --project $PROJECT \
  --region $REGION \
  --job_name gridlabd-init \
  --experiments use_runner_v2 \
  --input gs://jimmy_beam_bucket/wordscount/kinglear-1.txt \
  --output gs://jimmy_beam_bucket/demo_files/part \
  --temp_location gs://jimmy_beam_bucket/demo_files/temp \
  --experiments no_use_multiple_sdk_containers \
  --sdk_container_image $IMAGE_URI \
  --sdk_location container \
  --autoscalingAlgorithm NONE \
  --num_workers 8
GOOGLE_APPLICATION_CREDENTIALS="/Users/jimmyleu/Development/GCP/beamdataflow-366220-6acb2a6a2aa1.json"


	# $VERSION_DIR/bin/python3 -m pip install matplotlib Pillow pandas numpy networkx pytz pysolar PyGithub scikit-learn xlrd boto3
	# $VERSION_DIR/bin/python3 -m pip install IPython censusdata

python -m main --runner DirectRunner \
--input gs://jimmy_beam_bucket/PGE/AT0001.glm \
--output  gs://jimmy_beam_bucket/demo_files/gridlabd \
--direct_num_workers 0