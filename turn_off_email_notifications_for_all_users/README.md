# Purpose

By default, DataHub assumes all users who can accept proposals (i.e. those having Editor role) are interested in receiving
notifications about new proposals. This might be considered too verbose. This script will opt-out all the users from
email notifications.

# How it works

This script iterates over all users present in DataHub. If a CorpUserSettings aspect is found for a given user
(not all users have it, but those pulled from IdP will have it set automatically), then it is inspected, if one of the
defined sinks is `EMAIL`, it is removed from the aspect. Then the aspect is re-ingested. At the end of the execution,
script prints out statistics of what was done.

# Setup

```shell
# use at python version, at least, 3.10

# create a virtual environment
python -m venv venv
source venv/bin/activate

# install the dependencies
uv pip install -r ../requirements.txt

# initialize datahub to point at your instance, you will be required to have a valid PAT to authenticate
datahub init

# run the script
python ./opt_out_users_email_notifications.py
```
