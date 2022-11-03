import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount, Repeatedly
import json
import pandas as pd

# Replace with your service account path
service_account_path = '/Users/jimmyleu/Development/GCP/beamdataflow-366220-6acb2a6a2aa1.json'

print("Service account file : ", service_account_path)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path

# Replace with your input subscription id
input_subscription = 'projects/beamdataflow-366220/subscriptions/BeamTopic1-sub'


# Replace with your output subscription id
#output_topic = ''

options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

p = beam.Pipeline(options=options)


class ConvertDataToPVDataFrame(beam.DoFn):

    def process(self, element, column):

        site_id, data_list = element
        df = pd.DataFrame.from_dict(data_list, orient='columns')
        print(f"site_id : {site_id} ,length: {len(df)}")
        return site_id, df


def custom_timestamp(elements):
    unix_timestamp = elements[16].rstrip().lstrip()
    return beam.window.TimestampedValue(elements, int(unix_timestamp))


def decode_byte2string(element):
    # print(element)
    element = str(element.decode('utf-8'))

    return element


def encode_byte_string(element):
    # print(element)
    element = str(element)
    return element.encode('utf-8')


def extract_site_id(element):
    data_string = element
    # convert string to dict
    res = json.loads(data_string)
    if 'site_id' in res:
        site_id = res['site_id']
        del res['site_id']
        return site_id, res
    else:
        raise Exception("No site id in data")


def parse_site(element):
    site_id, data = element
    return site_id, 1


def print_row(element):
    print(element)
    return element


pubsub_data = (
    p
    | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription=input_subscription)
    | 'Deconde' >> beam.Map(decode_byte2string)
    | 'Extract site id' >> beam.Map(extract_site_id)
    # | 'Print row' >> beam.Map(print_row)
)

process_data = (
    pubsub_data
    # | 'Form k,v pair of (player_id, 1)' >> beam.Map(parse_site)
    | 'Window for player' >> beam.WindowInto(window.GlobalWindows(), trigger=Repeatedly(AfterCount(2)), accumulation_mode=AccumulationMode.DISCARDING)
    | 'Group players and their score' >> beam.GroupByKey()
    | 'Print raw' >> beam.Map(print_row)
    # | "Convert csv to dataframe" >> beam.ParDo(ConvertDataToPVDataFrame(), column="Power(W)")
    # | "Create solar data tools handler" >> beam.ParDo(CreateHandler())
    # | "Run solar data tools  pipeline" >> beam.ParDo(RunSolarDataToolsPipeline(), power_col="Power(W)", solver="MOSEK")
    # | "Get estimated capacity" >> beam.ParDo(GetEstimatedCapacity())
    # # | "Print" >> beam.Map(print_row)
    # | 'Write results' >> beam.io.WriteToText(outputs_prefix)
    # | 'Print datafra,e' >> beam.Map(print_row)
    # | 'Encode player info to byte string' >> beam.Map(encode_byte_string)

    # | 'Write player score to pub sub' >> beam.io.WriteToPubSub(output_topic)
)


result = p.run()
result.wait_until_finish()


# ('Site1',
#     [
#         {'Time': '2012-07-23 06:10:00',
#             'Energy(Wh)': 0, 'Power(W)': 2, 'data_id': '79a860ad-43b0-4f45-9ad7-c549d1cb32d4'},
#         {'Time': '2012-07-23 06:50:00',
#             'Energy(Wh)': 54, 'Power(W)': 175, 'data_id': 'fbdf2027-1e0f-4b1f-bd02-1789b9a7cdd5'},
#         {'Time': '2012-07-23 06:15:00',
#             'Energy(Wh)': 0, 'Power(W)': 2, 'data_id': 'bc9f62f5-328f-40fa-b6a8-85ad589bbc64'},
#         {'Time': '2012-07-23 06:20:00',
#             'Energy(Wh)': 0, 'Power(W)': 2, 'data_id': '5e875f45-5e21-4b57-9d57-8308581c9273'}
#     ]
#  )
# ('Site2',
#     [
#         {'Time': '2012-07-23 06:35:00',
#             'Energy(Wh)': 14, 'Power(W)': 108, 'data_id': 'dda28146-fee4-44ca-8e34-98657bfb598c'},
#         {'Time': '2012-07-23 06:40:00',
#             'Energy(Wh)': 25, 'Power(W)': 131, 'data_id': '1501587c-b485-4d76-a55a-d74055f71fb9'},
#         {'Time': '2012-07-23 06:45:00',
#             'Energy(Wh)': 39, 'Power(W)': 163, 'data_id': '3a4b181a-fa77-481d-962f-7ac209bc1bc0'},
#         {'Time': '2012-07-23 06:30:00',
#             'Energy(Wh)': 5, 'Power(W)': 49, 'data_id': 'd1bde4ca-483f-45af-8dfb-3e2dfdbb368a'}
#     ]
#  )
