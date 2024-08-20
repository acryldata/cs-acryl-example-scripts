# How to pull DataHub usage stats

# Purpose

Example of how customers can pull DataHub’s raw analytical data to perform analysis outside of DataHub’s current capabilities.

## How it works

The script makes use of the `openapi/v2/analytics/datahub_usage_events/_search` HTTP endpoint that the platform team created to surface all usage events in ElasticSearch we currently track in DataHub and saves them into a JSON file.

The script batches pulling down records in chunks of 10k events. This is the maximum that Elastic allows for a single request. 

Users of this script should update the `START_DATE` and `END_DATE` for the time period they want. I would **not** advise pulling more than 1 day worth of data at a time.

<aside>
💡 Acryl **reserves** the right to delete usage data older than 30 days

</aside>

## Setup

How to use it:

- Use python3
- Install DataHub cli: `pip install acryl-datahub`
- Run `datahub init` , this will guide you through configuring datahub cli to connect to your instance, the server url should be: https://<customer>.acryl.io/gms and adding the access token that you created.
- Update the python script to download the time period you are interested in (line 11 and 13). I would advise downloading in batches of 1 day.
- Run the script :)

