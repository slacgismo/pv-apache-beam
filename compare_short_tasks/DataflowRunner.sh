#!/bin/bash
echo "## Run pipeline with '--setup_file'."
PROJECT=$(gcloud config get-value project)
REGION=us-east1
export PYTHON_VERSION=$(python -c "import sys; print('{}.{}'.format(*sys.version_info[0:2]))")

# Build SDK container image
echo "## Build SDK container."

# IMAGE_URI="gcr.io/$PROJECT/pv_batch_python${PYTHON_VERSION}:2.42.0"
# gcloud builds submit . --tag $IMAGE_URI



# # Install apache-beam
# echo "## Install custom packages"
# pip install ./transformers



# # --------------------
# # GCD
# # --------------------
echo "## Run on google cloud platform"
# python -m main --runner DataflowRunner \
#   --project $PROJECT \
#   --region $REGION \
#   --job_name solardatatools \
#   --experiments use_runner_v2 \
#   --input gs://jimmy_beam_bucket/pvdata/10010.csv gs://jimmy_beam_bucket/pvdata/10059.csv gs://jimmy_beam_bucket/pvdata/10284.csv gs://jimmy_beam_bucket/pvdata/10838.csv gs://jimmy_beam_bucket/pvdata/10795.csv gs://jimmy_beam_bucket/pvdata/10807.csv gs://jimmy_beam_bucket/pvdata/10858.csv gs://jimmy_beam_bucket/pvdata/10877.csv\
#   --output gs://jimmy_beam_bucket/demo_files/part \
#   --temp_location gs://jimmy_beam_bucket/demo_files/temp \
#   --experiments no_use_multiple_sdk_containers \
#   --sdk_container_image $IMAGE_URI \
#   --sdk_location container \
#   --autoscalingAlgorithm NONE \
#   --numWorkers 5

# # --------------------
# # GCD
# # Autoscale
# # --------------------

python -m main --runner DataflowRunner \
  --project $PROJECT \
  --region $REGION \
  --job_name linearalgebra-100-10 \
  --experiments use_runner_v2 \
  --input 100 \
  --output gs://jimmy_beam_bucket/demo_files/short \
  --temp_location gs://jimmy_beam_bucket/demo_files/temp \
  --autoscalingAlgorithm THROUGHPUT_BASED \
  --max_num_workers 100






python -m main --runner DirectRunner \
--input 5 \
--output ./short_tasks \
--direct_num_workers 0


python -m main \
--runner PortableRunner \
--job_endpoint=localhost:8099 \
--environment_type=LOOPBACK \
--input 5 \
--output ./short_tasks \
--direct_num_workers 0

python -m main --runner DataflowRunner \
  --project $PROJECT \
  --region $REGION \
  --job_name linearalgebra-100-10 \
  --experiments use_runner_v2 \
  --input 100 \
  --output gs://jimmy_beam_bucket/demo_files/short \
  --temp_location gs://jimmy_beam_bucket/demo_files/temp \
  --autoscalingAlgorithm NONE \
  --num_workers 20


  python -m main --runner DataflowRunner \
  --project $PROJECT \
  --region $REGION \
  --job_name linearalgebra-short-task-1200-10-3 \
  --experiments use_runner_v2 \
  --input 1200 \
  --output gs://jimmy_beam_bucket/demo_files/short \
  --temp_location gs://jimmy_beam_bucket/demo_files/temp \
  --autoscaling_algorithm=NONE \
  --max_num_workers=8 \
  --num_workers=8


# https://cloud.google.com/dataflow/docs/guides/deploying-a-pipeline#fusion-optimization