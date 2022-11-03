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
import random
import time
import pandas as pd
from solardatatools import DataHandler
import google.cloud.storage.client as gcs
import os


def printout_data(element):
    data = element
    filename = data['name']
    bucket = data['bucket']
    print(data, filename, bucket)


def compose_shards(bucket, prefix, outfile, num_shards):
    num_shards = num_shards
    client = gcs.Client()
    # trigger on the last file only
    gcs_bucket = client.bucket(bucket)
    last_shard = '-%05d-of-%05d' % (num_shards - 1, num_shards)
    blobs = []
    for blob in client.list_blobs(bucket, prefix=prefix):
        filename = str(blob.name)
        if prefix not in filename:
            continue
        blobs.append(blob)
        print(filename)
    gcs_bucket.blob(outfile).compose(blobs)
    logging.info('Successfully created {}'.format(outfile))
    for blob in blobs:
        blob.delete()
    logging.info('Deleted {} shards'.format(len(blobs)))


class GetEstimatedCapacity(beam.DoFn):
    """
    Run the solar data tool pipeline on a PCollection of DataHandler objects
    ...

    Methods
    -------
    process(element):
        Run the process.
    """

    def process(self, element):
        (file, data_handler) = element
        yield {"file": file, "capacity_estimate": data_handler.capacity_estimate}


class CreateHandler(beam.DoFn):

    def process(self, element):

        (filename, df) = element
        dh = DataHandler(df)
        yield filename, dh


class RunSolarDataToolsPipeline(beam.DoFn):

    def process(self, element, power_col, solver):

        (filename, data_handler) = element
        data_handler.run_pipeline(
            power_col=power_col,
            min_val=-5,
            max_val=None,
            zero_night=True,
            interp_day=True,
            fix_shifts=True,
            density_lower_threshold=0.6,
            density_upper_threshold=1.05,
            linearity_threshold=0.1,
            clear_day_smoothness_param=0.9,
            clear_day_energy_param=0.8,
            verbose=False,
            start_day_ix=None,
            end_day_ix=None,
            c1=None,
            c2=500.0,
            solar_noon_estimator="com",
            correct_tz=True,
            extra_cols=None,
            daytime_threshold=0.1,
            units="W",
            solver=solver,
        )
        yield filename, data_handler


class ConverCSVToDataFrame(beam.DoFn):

    def process(self, element, column):
        n = random.randint(0, 1000)
        # time.sleep(5)
        logging.getLogger().warning('PARALLEL START? ' + str(n))
        data = element
        # print(f"type: {type(data)}")
        rawdata = data.encode('utf-8')
        filename = rawdata.decode('utf-8').replace(" ", "")
        print(f"file :{repr(filename)}")
        # print(repr(line))

        df = pd.read_csv(
            filename,
            index_col=0,
            parse_dates=[0],
            usecols=["Time", column],
        )
        yield filename, df


def print_row(element):
    print(element)
    return element


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
    input_file = known_args.input
    output_prefix = known_args.output
    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as p:

        # Read the text file[pattern] into a PCollection.
        lines = p | 'Read' >> ReadFromText(input_file)

        solardatatools = (
            lines
            | 'Reshuffle' >> beam.Reshuffle()
            | 'Create dataframe' >> beam.ParDo(ConverCSVToDataFrame(), "Power(W)")
            | 'Create solardatatools handler' >> beam.ParDo(CreateHandler())
            | 'Run solardatatools pipeline' >> beam.ParDo(RunSolarDataToolsPipeline(), "Power(W)", "MOSEK")
            | 'Get Estimation' >> beam.ParDo(GetEstimatedCapacity())
        )
        solardatatools | 'Write' >> WriteToText(
            known_args.output, num_shards=8)

    #  Compose shards files

    # compose_shards(
    #     bucket='jimmy_beam_bucket', prefix=output_prefix, outfile=output_prefix, num_shards=8)
    return


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
