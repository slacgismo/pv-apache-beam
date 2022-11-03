import logging
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

import pandas as pd
import random
from solardatatools import DataHandler


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
        (file, thread_name, data_handler) = element
        logging.getLogger().warning('PARALLEL END:  ' + str(thread_name))
        yield {"file": file, "thread_name": thread_name, "capacity_estimate": data_handler.capacity_estimate}


class CreateHandler(beam.DoFn):

    def process(self, element):

        (filename, thread_name, df) = element
        dh = DataHandler(df)
        yield filename, thread_name, dh


class RunSolarDataToolsPipeline(beam.DoFn):

    def process(self, element, power_col, solver):

        (filename, thread_name, data_handler) = element
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
        yield filename, thread_name, data_handler


class ConverCSVToDataFrame(beam.DoFn):

    def process(self, element, column):
        thread_name = random.randint(0, 1000)
        # time.sleep(5)
        logging.getLogger().warning('PARALLEL START : ' + str(thread_name))
        filename = element

        print(f"file :{repr(filename)}")
        # print(repr(line))
        df = pd.read_csv(
            filename,
            index_col=0,
            parse_dates=[0],
            usecols=["Time", column],
        )
        yield filename, thread_name, df
