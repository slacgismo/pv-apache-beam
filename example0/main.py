import logging
import sqlite3
import time
import argparse
from apache_beam import Create
from apache_beam import DoFn
from apache_beam import ParDo
from apache_beam import Pipeline
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.window import GlobalWindow
from apache_beam.utils.windowed_value import WindowedValue
from regex import P
from requests import options
from transformers.pv_transformers import ConverCSVToDataFrame, CreateHandler, RunSolarDataToolsPipeline, GetEstimatedCapacity
import pandas as pd
import numpy as np
from typing import List
from uuid import uuid4
import random
from apache_beam import window
from enum import Enum, auto
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount, Repeatedly


class Locations(Enum):
    Site1 = auto()
    Site2 = auto()
    # Site3 = auto()


def create_list_from_files(input_file) -> pd:
    df = pd.read_csv(
        input_file,
        index_col=0,
        low_memory=False
    )
    return df[:9]


def generate_random_locations() -> str:
    length_of_locations = len(Locations)
    return Locations(random.randint(1, length_of_locations)).name


def generate_simluation_pv_data(dataframe: pd) -> List[dict]:
    '''
    generate simulate data
    [
        {'Time': '2012-07-23 06:10:00',
            'Energy(Wh)': 0, 'Power(W)': 2, 'data_id': '9cb89c06-bcca-4a08-b0ce-d921009948a0', 'site_id': 'Site1'},
        {'Time': '2012-07-23 06:15:00',
            'Energy(Wh)': 0, 'Power(W)': 2, 'data_id': '6030506a-8326-4073-b1f2-31907fc7e265', 'site_id': 'Site3'}
    ]

    '''
    records = []
    for index, row in dataframe.iterrows():
        row = row.dropna()
        _data = dict(row)
        _data['data_id'] = str(uuid4())
        _data['site_id'] = generate_random_locations()
        records.append(_data)
    return records


def extract_site_id(element):
    site_id = element['site_id']
    del element['site_id']
    return site_id, element


def pair_site_id(element):
    site_id, data = element

    return site_id, data


def main(pipeline_args, path_args):
    inputs_pattern = path_args.input
    outputs_prefix = path_args.output

    df = create_list_from_files(input_file=inputs_pattern)

    records = generate_simluation_pv_data(df)
    print(records)

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = False
    with beam.Pipeline(options=options) as pipeline:
        pubsub_data = (
            pipeline
            | 'Gardening plants' >> beam.Create(records)
            | 'Extract site id' >> beam.Map(extract_site_id)
            | 'Pair site' >> beam.Map(pair_site_id)
            | 'Window for team' >> beam.WindowInto(window.GlobalWindows(), trigger=Repeatedly(AfterCount(2)), accumulation_mode=AccumulationMode.ACCUMULATING)
            | 'Group teams and their score' >> beam.GroupByKey()

            | 'Print' >> beam.Map(print)
        )


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        required=True,
                        help='Input file to write results to.')

    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output file to write results to.')

    path_args, pipeline_args = parser.parse_known_args()

    main(pipeline_args, path_args)
