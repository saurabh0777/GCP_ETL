from pyspark.sql import SparkSession
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/saurabhsrivastava/Desktop/Workspace/Project/gcp-data-pipeline/pepbigqueryexp-7b9d52749bfd.json"
os.environ["GCP_PROJECT"] = "pepbigqueryexp"


def read_bigquery_table_with_spark(project_id, dataset_id, table_id):

    spark = SparkSession.builder.appName("BigQueryReader") \
             .config('spark.jars','/Users/saurabhsrivastava/Desktop/Workspace/Software/spark-3.5-bigquery-0.36.0.jar') \
             .getOrCreate()

    # Read data from BigQuery table into a DataFrame
    df = spark.read.format("bigquery") \
        .option("table", f"{project_id}.{dataset_id}.{table_id}") \
        .load()

    # Show the DataFrame
    df.show()

    df.createOrReplaceTempView('test')

    # Run your SQL query on the DataFrame
    your_sql_query = "SELECT patientid,patienthash FROM test limit 4"
    result_df = spark.sql(your_sql_query)

    # Print the results
    result_df.show()

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    # Replace these values with your actual project ID, dataset ID, table ID, and credentials file path
    project_id = 'pepbigqueryexp'
    dataset_id = 's_rawinput'
    table_id = 'labreports_json'
    # Set the path to the JAR file in the script
    read_bigquery_table_with_spark(project_id, dataset_id, table_id)