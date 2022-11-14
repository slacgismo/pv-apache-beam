import argparse
import logging
import os

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions


from time import sleep
import random

from apache_beam.io.restriction_trackers import OffsetRange

DUMMY_FILE = './kinglear-1.txt'

class FileToWordsRestrictionProvider(beam.transforms.core.RestrictionProvider
                                     ):
    def initial_restriction(self, file_name):
        return OffsetRange(0, os.stat(file_name).st_size)

    def create_tracker(self, restriction):
        return beam.io.restriction_trackers.OffsetRestrictionTracker(
            offset_range=self.initial_restriction(file_name=DUMMY_FILE))

    def restriction_size(self, element, restriction):
        return restriction.size()


class FileToWordsFn(beam.DoFn):
    def process(
        self,
        file_name,
        # Alternatively, we can let FileToWordsFn itself inherit from
        # RestrictionProvider, implement the required methods and let
        # tracker=beam.DoFn.RestrictionParam() which will use self as
        # the provider.
            tracker=beam.DoFn.RestrictionParam(FileToWordsRestrictionProvider())):
        with open(file_name) as file_handle:
            file_handle.seek(tracker.current_restriction().start)
            while tracker.try_claim(file_handle.tell()):
                yield read_next_record(file_handle=file_handle)


def read_next_record(file_handle):
    line_number = file_handle.readline()
    logging.info(line_number)
    sleep(random.randint(1, 5))
    logging.info(f'iam done {line_number}')


def run(args, pipeline_args, file_name):
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        execute_pipeline(args, p, file_name)


def execute_pipeline(args, p, file_name):
    _ = (
          p |
          'Create' >> beam.Create([file_name]) |
          'Read File' >> beam.ParDo(FileToWordsFn(file_name=file_name))
    )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    """Build and run the pipeline."""
    parser = argparse.ArgumentParser()
    # to be added later

    args, pipeline_args = parser.parse_known_args()

    file_name = DUMMY_FILE
    run(args, pipeline_args, file_name)