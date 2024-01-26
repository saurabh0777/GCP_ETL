from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    '/Users/saurabhsrivastava/Desktop/Workspace/Project/gcp-data-pipeline/pepbigqueryexp-7b9d52749bfd.json')

client = bigquery.Client(credentials=credentials)
project = "pepbigqueryexp"
dataset_id = "s_rawinput"

dataset_ref = bigquery.DatasetReference(project, dataset_id)
table_ref = dataset_ref.table("labreports_json")
table = client.get_table(table_ref)

df = client.list_rows(table).to_dataframe()

print(df.head(5))