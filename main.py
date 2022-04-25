import boto3
import uuid
from datetime import datetime
import time
import random
import json
import random
from dotenv import load_dotenv
import os

load_dotenv()

PROFILE_NAME = os.getenv('PROFILE_NAME')
REGION_NAME = os.getenv('REGION_NAME')
STREAM_NAME = os.getenv('STREAM_NAME')


session = boto3.Session(profile_name=PROFILE_NAME)
client = session.client('kinesis',region_name=REGION_NAME)
partition_key = str(uuid.uuid4())
number_of_results = 500


# Streaming Data Configurations
frequency = int(input('Frequency of message generation in seconds: '))

def data_builder():
        dateToday = datetime.now()
        currTimeSuffix = dateToday.strftime("%Y-%m-%d %H:%M:%S")
        CurrencyList = ['INR', 'USD', 'GBP', 'CAD', 'AED', 'JPY']
        return {
                "SaleID":'GKS' + str("%03d" % random.randrange(2, 200)),
                "Product_ID" : 'P' + str("%03d" % random.randrange(1, 30)),
                "QuantitySold" : random.randrange(1, 10),
                "Vendor_ID" : 'GV' + str("%03d" % random.randrange(1, 20)),
                "SaleDate" : currTimeSuffix,
                "Sale_Amount" : '',
                "Currency" : random.choice(CurrencyList)
        }


while True:

        message = json.dumps(data_builder())
        print(message)
        client.put_record(
                StreamName=STREAM_NAME ,
                Data=message,
                PartitionKey=partition_key)
        
        time.sleep(frequency)


