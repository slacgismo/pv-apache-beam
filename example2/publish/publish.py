import os
import time
from google.cloud import pubsub_v1
import pandas as pd
from enum import Enum, auto
from typing import List
from uuid import uuid4
import random
import json

from zmq import DEALER


class Locations(Enum):
    Site1 = auto()
    Site2 = auto()
    # Site3 = auto()


def create_list_from_files(input_file) -> pd:
    df = pd.read_csv(
        input_file,
        index_col=0,
        low_memory=False
    )
    return df[:10]


def generate_random_locations() -> str:
    length_of_locations = len(Locations)
    return Locations(random.randint(1, length_of_locations)).name


def generate_simluation_pv_data(dataframe: pd) -> List[dict]:
    '''
    generate simulate data
    [
        {'Time': '2012-07-23 06:10:00',
            'Energy(Wh)': 0, 'Power(W)': 2, 'data_id': '9cb89c06-bcca-4a08-b0ce-d921009948a0', 'site_id': 'Site1'},
        {'Time': '2012-07-23 06:15:00',
            'Energy(Wh)': 0, 'Power(W)': 2, 'data_id': '6030506a-8326-4073-b1f2-31907fc7e265', 'site_id': 'Site3'}
    ]

    '''
    records = []
    for index, row in dataframe.iterrows():
        row = row.dropna()
        _data = dict(row)
        _data['data_id'] = str(uuid4())
        _data['site_id'] = generate_random_locations()
        records.append(_data)
    return records


def publisher(data_list: list = None, delay: int = 1, pubsub_topic: str = None) -> None:
    # create publisher
    publisher = pubsub_v1.PublisherClient()

    # publish row by row
    for row in data_list:
        # convert dictionary to string type
        string_data = json.dumps(row)
        # convert string to bytes utf-8
        event_data = bytes(string_data, 'utf-8')
        print('Publishing {0} to {1}'.format(event_data, pubsub_topic))
        publisher.publish(pubsub_topic, event_data)
        time.sleep(delay)


def main(input_file) -> None:
    project = 'beam-tutorial-366117'

    # Replace 'my-topic' with your pubsub topic
    pubsub_topic = 'projects/beamdataflow-366220/topics/BeamTopic1'

    # Replace 'my-service-account-path' with your service account path
    path_service_account = '/Users/jimmyleu/Development/GCP/beamdataflow-366220-6acb2a6a2aa1.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account
    df = create_list_from_files(input_file=input_file)

    records = generate_simluation_pv_data(df)

    publisher(data_list=records, pubsub_topic=pubsub_topic, delay=0.5)


if __name__ == "__main__":
    input_file = "./10010.csv"
    main(input_file=input_file)
