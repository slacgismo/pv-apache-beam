#!/bin/bash

# Install apache-beam
echo "## Install custom packages"
pip install ./transformers

# Run pipeline
# --------------------
# DirectRunner
# --------------------
echo "## Direct Runner"
python -m main --runner DirectRunner --input gs://jimmy_beam_bucket/pvdata/10010.csv --output gs://jimmy_beam_bucket/demo_files/results 





