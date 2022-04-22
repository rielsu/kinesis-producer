import boto3
import uuid
import datetime
import time
import random
import json
client = boto3.client('kinesis', region_name='us-east-1')
partition_key = str(uuid.uuid4())
number_of_results = 500

def data_builder():
    return {
            "eventId": str(uuid.uuid4()),
            "eventDate": str(datetime.now()),
                "businessProcess": "PolicyEnrollment",
                "eventClass": "CaseEvent",
                "eventType": "CaseChange",
                "eventStatus": "Success",
                "market": "Broad",
                "sourceSystem": "Intake",
                "sourceState": {
                "3rdParty": "LexisNexis",
                "3rdPartyService": "Risk Classifier"
                }
    }

while True:

        message = json.dumps(data_builder())
        client.put_record(
                StreamName='dream',
                Data=message,
                PartitionKey=partition_key)
        time.sleep(random.uniform(0, 1))