from google.cloud import bigquery
from google.cloud import bigquery_storage
from google.oauth2 import service_account
import pandas as pd

# Set up authentication (Application Default Credentials or service account key file)
# See https://cloud.google.com/docs/authentication/getting-started

# Specify project ID, dataset ID, and table ID
project_id ='pepbigqueryexp'
dataset_id = 's_rawinput'
table_id = 'labreports_json'

#`pepbigqueryexp.s_rawinput.labreports_json`

credentials = service_account.Credentials.from_service_account_file(
    '/Users/saurabhsrivastava/Desktop/Workspace/Project/gcp-data-pipeline/pepbigqueryexp-7b9d52749bfd.json')

bq_client = bigquery.Client(credentials=credentials)
#bqstorage_client = bigquery_storage.BigQueryReadClient(credentials=credentials)


datasets = list(bq_client.list_datasets())

print("Datasets:")
for dataset in datasets:
    print(f"- {dataset.dataset_id}")

    # List tables within each dataset
    tables = list(bq_client.list_tables(dataset))

    print("  Tables:")
    for table in tables:
        print(f"    - {table.table_id}")

print("Completed listing datasets and tables.")