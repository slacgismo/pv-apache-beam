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
from transformers.pv_transformers import ConverCSVToDataFrame, CreateHandler, RunSolarDataToolsPipeline, GetEstimatedCapacity


def print_row(element):
    print(element)


def main(pipeline_args, path_args):
    inputs_pattern = path_args.input
    outputs_prefix = path_args.output

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True
    p = Pipeline(options=options)

    attendance_count = (
        p
        | 'Read lines' >> beam.Create(inputs_pattern)
        | 'Reshuffle' >> beam.Reshuffle()
        # | 'print_hello' >> beam.Map(print_hello)
        | "Convert csv to dataframe" >> beam.ParDo(ConverCSVToDataFrame(), column="Power(W)")
        | "Create solar data tools handler" >> beam.ParDo(CreateHandler())
        | "Run solar data tools  pipeline" >> beam.ParDo(RunSolarDataToolsPipeline(), power_col="Power(W)", solver="MOSEK")
        | "Get estimated capacity" >> beam.ParDo(GetEstimatedCapacity())
        # | "Print" >> beam.Map(print_row)
        | 'Write results' >> beam.io.WriteToText(outputs_prefix)
    )

    p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', '--list', dest='input', nargs='+',
                        help='Input file  list to process.', required=True)

    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output file to write results to.')

    path_args, pipeline_args = parser.parse_known_args()

    main(pipeline_args, path_args)
