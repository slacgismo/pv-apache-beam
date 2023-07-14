import logging
import numpy as np
import pandas as pd
import random
import apache_beam as beam
from solardatatools import DataHandler


class ConvertCSVToDataFrame(beam.DoFn):
    """
    Convert input file from CSV to DF
    ...

    Methods
    -------
    process(element, column):
        Convert input file to DF with the time and the power column as included columns.
    """
    def process(self, element, column):
        thread_name = random.randint(0, 1000)
        logging.getLogger().warning('PARALLEL START : ' + str(thread_name))
        filename = element

        df = pd.read_csv(
            filename,
            # index_col=0,
            # parse_dates=True,
            # usecols=["ts", column],
        )

        yield filename, column, thread_name, df


class CreateHandler(beam.DoFn):
    """
    Create the DataHandler objects
    ...

    Methods
    -------
    process(element):
        Create DataHandler object dh given the DataFrame df.
    """
    def process(self, element):
        (filename, column, thread_name, df) = element
        data_handler = DataHandler(df, convert_to_ts=True)
        yield filename, column, thread_name, data_handler


class RunPipeline(beam.DoFn):
    """
    Run the solardatatools pipeline on a PCollection of DataHandler objects
    ...

    Methods
    -------
    process(element):
        Run the process.
    """
    def process(self, element, solver):
        (filename, power_col, thread_name, data_handler) = element
        data_handler.run_pipeline(
            power_col=data_handler.keys[0][-1], #power_col,
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


class GetReport(beam.DoFn):
    """
    Get report from DataHandler pipeline run
    ...

    Methods
    -------
    process(element):
        Get DH results.
    """
    def process(self, element):
        (file, thread_name, data_handler) = element
        logging.getLogger().warning('PARALLEL END:  ' + str(thread_name))
        yield {
            "file": file,
            "thread_name": thread_name,
            "length": data_handler.num_days / 365,
            "capacity": (
                data_handler.capacity_estimate
                if data_handler.power_units == "kW"
                else data_handler.capacity_estimate / 1000
            ),
            "sampling": data_handler.data_sampling,
            "quality score": data_handler.data_quality_score,
            "clearness score": data_handler.data_clearness_score,
            "inverter clipping": data_handler.inverter_clipping,
            "clipped fraction": (
                    np.sum(data_handler.daily_flags.inverter_clipped)
                    / len(data_handler.daily_flags.inverter_clipped)
            ),
            "capacity change": data_handler.capacity_changes,
            "data quality warning": data_handler.normal_quality_scores,
            "time shift correction": (
                data_handler.time_shifts if data_handler.time_shifts is not None else False
            ),
            "time zone correction": data_handler.tz_correction,
        }

