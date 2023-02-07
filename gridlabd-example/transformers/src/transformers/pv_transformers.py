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
import os


class RunGridlabd(beam.DoFn):
    """Parse each line of input text into words."""

    def process(self, element):
        filename = element
        

        n = random.randint(0, 1000)
        # time.sleep(5)
        # logging.getLogger().warning('PARALLEL START : ' + str(n))
        if os.path.exists(filename):
            logging.getLogger().warning(f"==== {filename} exit ========")
        else:
            logging.getLogger().warning(f"==== Not exit {filename} ========")
        # run gridlabd 
        # command = f"echo {filename} 1>>data.csv 2>>gridlabd.log"
        
        command = f"gridlabd {filename} 1>>data.csv 2>>gridlabd.log"
        returncode = subprocess.call(command, shell=True)
        logging.getLogger().warning(f"*********** returncode {returncode} ***************")
        # if returncode != 0 :
        #     errorObject = open("gridlabd.log","r")
        #     error = errorObject.read()
        #     logging.getLogger().warning(f"==== Error ====: {error}")
        #     errorObject.close()
        #     # logging.getLogger().warning('PARALLEL END : ' + str(n))
        #     return error
        if os.path.exists("gridlabd.log"):
            logfile = open("gridlabd.log","r")
            log  = logfile.read()
            logging.getLogger().warning(f"== log ===: {log} ======")
            logfile.close()
            
            return log
        
        if os.path.exists("data.csv"):
            fileObject = open("data.csv","r")
            data  = fileObject.read()
            logging.getLogger().warning(f"== Data ===: {data} ======")
            fileObject.close()
            

            logging.getLogger().warning('PARALLEL END : ' + str(n))
            
            return data

