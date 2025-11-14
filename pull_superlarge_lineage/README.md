# Pull arbitrary large lineage scrip

## Problem this script is addressing

Currently, graphQL endpoint `scrollLineage` has a hard limitation of 40.000 edges present in the lineage for the resolver
to provide results. For vast lineage graphs, this limit might be hit, even if retrieving only 1st degree dependencies.
This script uses OpenAPI's `openapi/v3/relationship` endpoint, to retrieve URNs of dependent entities. Arbitrary maximum
depth of relations might be retrieved, but any lineage beyond 1st degree might take unreasonably long to be resolved.

## Script usage

Please note that the intended users of this script need to understand how it works and might need to modify the code.
Script bases on low-level concept of `relationships`. Details on the metadata model used by DataHub can be found
in the [official documentation](https://docs.datahub.com/docs/metadata-modeling/metadata-model). Relationships for,
for example, a dataset can be found [here](https://docs.datahub.com/docs/generated/metamodel/entities/dataset#relationships).

Script has sets of `RELATIONSHIP_TYPES`. By default, it is initialized to find equivalent of downstream lineage nodes.
There is a commented version of that variable in the code which would effectively cause script to find upstream lineage
nodes. The script will automatically use endpoint and credentials found in `~/.datahubenv`. Make sure it is initialized,
before running the script. The class implementing the logic has to be instantiated before the usage, for example:

```python
retriever = LineageOpenAPIRetriever()
retriever.get_lineage("urn:li:dataset:(urn:li:dataPlatform:s3,raw-data-bucket/orders/2024/customer_orders.csv,PROD)", "test.log", max_depth=5)
```

This will retrieve nodes related to the provided URN, up to 5th degree of depth. The actual URNs, with some informative
lines (starting with `#`) will be saved in the provided file. There will be some metrics/info also printed to `stdout`.

> **! Important note on performance !**<br>
> Retrieving **1st degree** relations nodes, even if that set is huge (e.g. exceeds 100.000) is relative fast, as it will be
> done by just several scrolling requests (currently script uses batches of 3000, so 34 calls will be enough). However,
> retrieving **2nd degree** relations nodes, will require at least 2 separate calls to the OpenAPI - in case 1st degree
> relations had 100.000 nodes, it would mean 200.000 API calls - this will take a lot of time to execute!


## Further improvements

Certainly script could be improved and extended, depending on desired use-cases:

1. Considering possible long time of execution, a feature to restart execution, from the last checkpoint, saved to the file,
could be found very much useful.
2. Currently, script focuses on limited set of relations between datasets, dashboards and dataJobs/dataFlows. This might
be, easily, extended to use more relations, as well as, entities (e.g. those related to ML models).
