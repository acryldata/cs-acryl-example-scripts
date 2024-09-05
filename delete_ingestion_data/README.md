This script will delete all entities **created** by an ingestion source (identified by it's urn).
This will pull all entities urns in DataHub and check each's system metadata to see if the key aspect was created by the ingestion urn that was provided.

### Requirements:
- DataHub Cli v0.13.3.6 
- DataHub Server version v0.14+ for OSS and 0.3.4+ for SaaS
- Install Rich: `pip install rich`