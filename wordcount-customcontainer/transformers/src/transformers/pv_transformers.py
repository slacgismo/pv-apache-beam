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
