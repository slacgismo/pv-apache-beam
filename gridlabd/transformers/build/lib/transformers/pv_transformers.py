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
import re
import pandas as pd
import random
from solardatatools import DataHandler
import numpy as np


class WordExtractingDoFn(beam.DoFn):
    """Parse each line of input text into words."""

    def process(self, element):
        """Returns an iterator over the words of this element.
        The element is a line of text.  If the line is blank, note that, too.
        Args:
          element: the element being processed
        Returns:
          The processed element.
        """
        import random
        import time
        n = random.randint(0, 1000)
        # time.sleep(5)
        logging.getLogger().warning('PARALLEL START : ' + str(n))
        delay = 1
        start_time = float(time.time())
        while True:
            A = np.array([[4, 3, 2], [-2, 2, 3], [3, -5, 2]])
            B = np.array([25, -10, -4])
            answer = np.linalg.inv(A).dot(B)
            end_time = float(time.time())

            duration = int(end_time - start_time)
            if duration >= delay:
                break
        words = re.findall(r'[\w\']+', element, re.UNICODE)

        logging.getLogger().warning('PARALLEL END : ' + str(n))
        return words


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


# class CreateHandler(beam.DoFn):

#     def process(self, element):

#         (file, df_list) = element

#         dh = DataHandler(df_list[0])
#         yield file, dh


class ConverCSVToDataFrame(beam.DoFn):

    def process(self, element, column):
        n = random.randint(0, 1000)
        # time.sleep(5)
        logging.getLogger().warning('PARALLEL START? ' + str(n))
        data = element
        # print(f"type: {type(data)}")
        rawdata = data.encode('utf-8')
        data_String = rawdata.decode('utf-8').replace(" ", "")
        print(f"file :{repr(data_String)}")
        # print(repr(line))

        df = pd.read_csv(
            data_String,
            index_col=0,
            parse_dates=[0],
            usecols=["Time", 'Power(W)'],
        )
        dh = DataHandler(df)
        dh.run_pipeline(
            power_col=column,
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
            solver='MOSEK',
        )
        logging.getLogger().warning('PARALLEL END? ' + str(n))

        yield data_String, df


# class ConverCSVToDataFrameList(beam.DoFn):

#     def process(self, element, column):

#         files = element
#         dataframe_list = []
#         for file in files:

#         df = pd.read_csv(
#             file,
#             index_col=0,
#             parse_dates=[0],
#             usecols=["Time", column],
#         )

#         yield file, df


# class CreateHandler(beam.DoFn):

#     def process(self, element):

#         (file, df) = element
#         dh = DataHandler(df)
#         yield file, dh


class RunSolarDataToolsPipeline(beam.DoFn):

    def process(self, element, power_col, solver):

        (file, data_handler) = element
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
        yield file, data_handler