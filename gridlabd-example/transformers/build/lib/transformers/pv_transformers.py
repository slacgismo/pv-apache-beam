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
import random
import subprocess



class RunGridlabd(beam.DoFn):
    """Parse each line of input text into words."""

    def process(self, element):
        filename = element
        

        n = random.randint(0, 1000)
        # time.sleep(5)
        logging.getLogger().warning('PARALLEL START : ' + str(n))
        
        # run gridlabd 
        command = f"gridlabd {filename} 1>>data.csv 2>>gridlabd.log"
        returncode = subprocess.call(command, shell=True)
        if returncode != 0 :
            errorObject = open("gridlabd.log","r")
            error = errorObject.read()
            print(f"error: {error}")
            errorObject.close()
            logging.getLogger().warning('PARALLEL END : ' + str(n))
            return error
        
        fileObject = open("data.csv","r")
        data  = fileObject.read()
        print(f"data: {data}")
        fileObject.close()
        

        logging.getLogger().warning('PARALLEL END : ' + str(n))
        return data

