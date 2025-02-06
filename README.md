# CS Acryl Example Scripts
Example scripts shared publically for using Acryl Cloud

# Basic Python CLI Setup
```shell
pip install --upgrade uv
uv venv venv --python 3.10
source venv/bin/activate
uv pip install -r ./requirements.txt
```
- Run `datahub init` , this will guide you through configuring datahub cli to  connect to your instance, the server url should be: https://<customer>.acryl.io/gms and adding the access token that you created

# Find Recommended CLI version to use with your server version
- In our documentation you'll find a page with release notes like https://datahubproject.io/docs/managed-datahub/release-notes/v_0_3_8. Go to the release notes for your server version and use the recommended CLI unless you have been advised otherwise.


# Internal guide to contribute
Run these
```
ruff format
ruff check --fix
```