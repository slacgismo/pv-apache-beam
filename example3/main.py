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
from transformers.pv_transformers import ConverCSVToDataFrame, RunSolarDataToolsPipeline, GetEstimatedCapacity, CreateHandler
from solardatatools import DataHandler
import pandas as pd
from apache_beam.io.fileio import MatchFiles


def print_row(element):
    print(element)
    return element


# def convert_file_to_dataframe(element):
#     data = element
#     # print(f"type: {type(data)}")
#     rawdata = data.encode('utf-8')
#     data_String = rawdata.decode('utf-8').replace(" ", "")
#     print(repr(data_String))
#     # print(repr(line))

#     df = pd.read_csv(
#         data_String,
#         index_col=0,
#         parse_dates=[0],
#         usecols=["Time", 'Power(W)'],
#     )
#     return data_String, df


def main(pipeline_args, path_args):
    inputs_pattern = path_args.input
    outputs_prefix = path_args.output
    filename = inputs_pattern[0]
    options = PipelineOptions(pipeline_args)
    # options.view_as(SetupOptions).save_main_session = False
    p = Pipeline(options=options)

    attendance_count = (
        p
        | 'Read file name from txt' >> beam.io.textio.ReadFromText(filename)
        # | 'Convert csv to dataframe' >> beam.Map(convert_file_to_dataframe)
        # | 'print_hello' >> beam.Map(print_hello)
        | 'Reshuffle' >> beam.Reshuffle()
        | "Convert csv to dataframe" >> beam.ParDo(ConverCSVToDataFrame(), column="Power(W)")

        # | "Print" >> beam.Map(print_row)
        # | 'Group by filename' >> beam.GroupByKey()
        # | "Create solar data tools handler" >> beam.ParDo(CreateHandler())
        # | "Run solar data tools  pipeline" >> beam.ParDo(RunSolarDataToolsPipeline(), power_col="Power(W)", solver="MOSEK")
        # | "Get estimated capacity" >> beam.ParDo(GetEstimatedCapacity())
        # | 'Write results' >> beam.io.WriteToText(outputs_prefix)
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
# not scale over 1 worker
# https://stackoverflow.com/questions/66053938/why-google-cloud-dataflow-doesnt-scale-to-target-workers-with-autoscaling-enabl
# https://stackoverflow.com/questions/65608212/control-parallelism-in-apache-beam-dataflow-pipeline
# https://stackoverflow.com/questions/64908691/how-does-dataflow-perform-parallel-processing
# https://www.youtube.com/watch?v=ss6SaNBHLGc
# https://www.youtube.com/watch?v=owTuuVt6Oro
# https://stackoverflow.com/questions/62563137/apache-beam-pipeline-step-not-running-in-parallel-python
# https://stackoverflow.com/questions/68821162/how-to-enable-parallel-reading-of-files-in-dataflow
# https://stackoverflow.com/questions/70514139/reading-multiple-csv-files-and-merging-them-into-one-using-apache-beam
# https://www.youtube.com/watch?v=KsZo1ZPXaJk
# https://stackoverflow.com/questions/70575165/how-to-execute-custom-splittable-dofn-in-parallel
# https://github.com/apache/beam/blob/master/sdks/python/apache_beam/testing/synthetic_pipeline.py
# https://jayendrapatil.com/google-cloud-dataflow/
