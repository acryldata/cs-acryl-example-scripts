import os
import json 
from datetime import datetime
from datahub.cli.cli_utils import (
    get_session_and_host
)

# Inputs
OUTPUT_FILE = "output.json"

START_DATE = "14/11/2023 00:00:00,00"

END_DATE = "14/11/2023 23:59:59,99"

PAGE_SIZE=10000

session, gms_host = get_session_and_host()

url = f"{gms_host}/openapi/v1/analytics/datahub_usage_events/_search"

def get_timestamp(date_str):
    dt_obj = datetime.strptime(date_str, '%d/%m/%Y %H:%M:%S,%f')
    return int(dt_obj.timestamp() * 1000)

def pull_analytics(search_after=None):
    
    headers = {
        'Content-Type': 'application/json',
        'accept': 'application/json',
    }

    payload_obj = {
        "size": PAGE_SIZE,
        "query":{
        "range":{
            "timestamp":{
                "gte": get_timestamp(START_DATE),
                "lt": get_timestamp(END_DATE),
        }}},
        "sort": [{"timestamp": "desc"}],
    }
        
    if search_after:
        payload_obj["search_after"] = [str(search_after)]

    payload = json.dumps(payload_obj)
    response = session.post(url, data=payload, headers=headers)

    response.raise_for_status()

    response_json = response.json()
    hits = response_json['hits']['hits']
    if hits:
        with open(OUTPUT_FILE, 'a') as file:
            # remove brackets
            for elem in hits:
                json.dump(elem, file)
                file.write(",\n")
        last_sort_value = hits[-1]['sort'][0]
        return last_sort_value
    else:
        return None

# Create a file with the start of a json list object
with open(OUTPUT_FILE, 'w') as file:
    file.write('[')

# Initial query
last_sort_value = pull_analytics()

# Paginate until no more results
while last_sort_value:
    last_sort_value = pull_analytics(last_sort_value)

# Remove last 2 chars from file ( a comma and a new line)
with open(OUTPUT_FILE, 'rb+') as filehandle:
    filehandle.seek(-2, os.SEEK_END)
    filehandle.truncate()

# Close the json list object
with open(OUTPUT_FILE, 'a') as file:
    file.write(']')

