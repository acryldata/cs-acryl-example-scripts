## Circuit Break Toolkit

In this directory, you'll find API calls that are useful for implementing circuit break functionality:

- examples.py is the driver
- graphql/ holds the graphQL queries that are being used along with a .py file for getter functions for the graphQL variables

Steps to run:
1. update the gms endpoint to your instance and run `export DATAHUB_TOKEN=XXX` in your terminal with your token value
```
gms_endpoint = "https://longtailcompanions.acryl.io/gms"
token = os.getenv("DATAHUB_TOKEN")
```

2. update the urns in the examples.py with the appropriate urns you'd like to use for each call

3. run the script.


## Other Notes

Please look through the .gql files themselves as these API calls can be extended to include more (or less) information. If you have
questions about how to do that, reach out to your Acryl Representative.
