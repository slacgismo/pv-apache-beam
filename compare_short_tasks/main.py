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
import time

class LinearAlgebra(beam.DoFn):
    """Parse each line of input text into words."""

    def process(self, element, delay):
        """Returns an iterator over the words of this element.
        The element is a line of text.  If the line is blank, note that, too.
        Args:
          element: the element being processed
        Returns:
          The processed element.
        """
        import random
        import time
        index = element
        n = random.randint(0, 1000)
        # time.sleep(5)
        logging.getLogger().warning('PARALLEL START : ' + str(n))
        delay = delay
        start_time = float(time.time())
        duration = 0
        while True:
            A = np.array([[4, 3, 2], [-2, 2, 3], [3, -5, 2]])
            B = np.array([25, -10, -4])
            answer = np.linalg.inv(A).dot(B)
            end_time = float(time.time())

            duration = int(end_time - start_time)
            if duration >= delay:
                break

        logging.getLogger().warning(f'PARALLEL END : {str(n)} , Duraction: {duration}')
        yield (n, start_time, end_time, duration)


def print_row(element):
    print(element)
    return element


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='5',
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

    input_num = int(known_args.input)
    print(input_num)
    num__lists = [ num for num in range(0,input_num) ]

    # The pipeline will be run on exiting the with block.

    main_start = float(time.time())
    delay = 10
    with beam.Pipeline(options=pipeline_options) as p:

        # Read the text file[pattern] into a PCollection.
        # lines = p | 'Read' >> ReadFromText(known_args.input)
        lines = (p 
                | 'Create ' >> beam.Create(num__lists)
                | 'Reshuffle' >> beam.Reshuffle()
                | 'Linear algebra' >> (beam.ParDo(LinearAlgebra(), delay))
                # | 'Print' >> beam.Map(print)
            )
        # def format_result(n, start_time,end_time,duration):
        #     return f'thread: {n}, start_time: {start_time},end_time: {end_time}, duration: {duration}'

        # output = lines | 'Format' >> beam.MapTuple(format_result)

        # # Write the output using a "Write" transform that has side effects.
        # # pylint: disable=expression-not-assigned
        # output | 'Write' >> WriteToText(known_args.output)

    main_end = float(time.time())
    total_duraiton = int(main_end - main_start)
    logging.getLogger().warning(f"====== main_start: {main_start} main_end :{main_end}===========")
    logging.getLogger().warning(f"====== Total duration: {total_duraiton} ===========")

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
