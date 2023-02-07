#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A word-counting workflow."""

# pytype: skip-file

# beam-playground:
#   name: WordCount
#   description: An example that counts words in Shakespeare's works.
#   multifile: false
#   pipeline_options: --output output.txt
#   context_line: 44
#   categories:
#     - Combiners
#     - Options
#     - Quickstart
#   complexity: MEDIUM
#   tags:
#     - options
#     - count
#     - combine
#     - strings

import argparse
import logging
import re
import numpy as np
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.fileio import MatchFiles, ReadMatches
from solardatatools import DataHandler
import random
import time
import pandas as pd
# from transformers.pv
# from transformers.pv_transformers import ConvertCSVToDataFrame, RunSolarDataToolsPipeline, GetEstimatedCapacity, CreateHandler
from transformers.pv_transformers import RunSolarDataToolsPipeline, GetEstimatedCapacity


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://dataflow-samples/shakespeare/kinglear.txt',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session

    input_dir = known_args.input
    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as p:

        # Read the text file[pattern] into a PCollection.
        # lines = p | 'Read' >> ReadFromText(known_args.input)
        filter_files = (
            p
            | 'Collect CSV files' >> beam.io.fileio.MatchFiles(input_dir + "/*.csv")
            | 'Match files' >> beam.io.fileio.ReadMatches()
            | 'Get file name from metadata' >> beam.Map(lambda file: (file.metadata.path))
        )
        solardatatools = (
            filter_files
            | 'Reshuffle' >> beam.Reshuffle()
            | 'Convert CSV to Dataframe' >> (beam.ParDo(ConvertCSVToDataFrame(), "Power(W)"))
            | 'Create solardatatools handler' >> beam.ParDo(CreateHandler())
            | 'Run solardatatools pipeline' >> beam.ParDo(RunSolarDataToolsPipeline(), "MOSEK")
            | 'Get Estimation' >> beam.ParDo(GetEstimatedCapacity())
        )

        solardatatools | 'Write' >> beam.io.WriteToText(
            known_args.output, num_shards=8)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
