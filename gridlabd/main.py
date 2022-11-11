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


import argparse
import logging
import subprocess

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from transformers.pv_transformers import RunGridlabd
import re
import random
import time
import os 
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/Users/jimmyleu/Development/GCP/beamdataflow-366220-6acb2a6a2aa1.json"





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
        lines = p | 'Create' >> beam.Create([known_args.input])

        counts = (
            lines
            # | 'Reshuffle' >> beam.Reshuffle()
            | 'Split' >> (beam.ParDo(RunGridlabd()))
            # | 'Print' >> beam.Map(print)
            # | 'Split' >> (beam.ParDo(WordExtractingDoFnNoNumpy()).with_output_types(str))
            # | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
            # | 'GroupAndSum' >> beam.CombinePerKey(sum)
        )

        # Format the counts into a PCollection of strings.
        # def format_result(word, count):
        #     return '%s: %d' % (word, count)

        # output = counts | 'Format' >> beam.MapTuple(format_result)

        # Write the output using a "Write" transform that has side effects.
        # pylint: disable=expression-not-assigned
        counts | 'Write' >> WriteToText(known_args.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
