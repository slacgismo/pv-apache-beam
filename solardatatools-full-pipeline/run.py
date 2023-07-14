import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.fileio import MatchFiles
from sdt_transformers import ConvertCSVToDataFrame, CreateHandler, RunPipeline, GetReport


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the SDT pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
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
            | 'Convert CSV to Dataframe' >> (beam.ParDo(ConvertCSVToDataFrame(), "ac_power_01"))
            | 'Create SDT DataHandler' >> beam.ParDo(CreateHandler())
            | 'Run SDT pipeline' >> beam.ParDo(RunPipeline(), "MOSEK")
            | 'Get Report' >> beam.ParDo(GetReport())
        )

        solardatatools | 'Write' >> beam.io.WriteToText(
            known_args.output, num_shards=1)


if __name__ == '__main__':
    #logging.basicConfig(filename='example.log', level=logging.ERROR)
    logging.getLogger().setLevel(logging.ERROR)
    run()