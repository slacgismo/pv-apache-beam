#!/bin/bash

# Install apache-beam
echo "## Install custom packages"
pip install ./transformers

# Run pipeline
# --------------------
# DirectRunner
# --------------------
echo "## Direct Runner"
python -m main --runner DirectRunner \
--input gs://jimmy_beam_bucket/pvdata/inputfiles.txt \
--output gs://jimmy_beam_bucket/demo_files/results 
--direct_num_workers 0




