This script will transformation tags to group ownership information based on a pre-established pattern. 
3 different tags prefixed with `data__producer__owner__`:
 - `data__producer__owner__email:<email of the data producing group>`
 - `data__producer__owner__team__name:<name of the data producing group>`
 - `data__producer__owner__slack__channel:<slack channel of the data producing group>`

This is for demonstration purposes, adjust as necessary.

### Requirements:
- DataHub Cli v0.13.3.6+ 
- DataHub Server version v0.14+ for OSS and 0.3.4+ for SaaS