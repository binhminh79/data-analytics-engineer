import os
from google.oauth2.service_account import Credentials
from google.cloud import storage as ggstorage
from google.cloud import bigquery

class GoogleCloudHandler:
    def __init__(self, key_file, bucket_name):
        self.__key_file = key_file
        self.bucket_name = bucket_name
        ggcredentials = Credentials.from_service_account_file(key_file)
        self.__storage_client = ggstorage.Client(credentials = ggcredentials)
        self.__bigquery_client = bigquery.Client(credentials = ggcredentials)
        self.__bucket = None
        return

    def ensure_bucket(self):
        self.__bucket = self.__storage_client.bucket(self.bucket_name)
        if self.__bucket == None:
            self.__bucket = self.__storage_client.create_bucket(self.bucket_name)
        print(f"Bucket {self.__bucket.name} created.")
        return

    def upload(self, file_name, remote_path):
        if self.__bucket == None:
            self.ensure_bucket()
        if self.__bucket == None:
            print(f"Cannot create bucket {self.bucket_name}")
            return

        upload_filename = f"{remote_path}{os.path.basename(file_name)}"  # the name of blob
        blob = self.__bucket.blob(upload_filename)
        blob.upload_from_filename(file_name)

        print(f"File {blob.name} is uploaded.")
        return

    def gsutil_to_bigquery(self, gsutil, table_id, schema):
        bq_schema = list()
        for key, val in schema.items():
            bq_schema.append(bigquery.schema.SchemaField(key, val))
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows = 1,
            autodetect = True,
            write_disposition = bigquery.WriteDisposition.WRITE_APPEND,
            schema = bq_schema,
        )
        job = self.__bigquery_client.load_table_from_uri(
            source_uris=gsutil,
            destination=table_id,
            job_config=job_config
        )
        job.result()  # Waits for the job to complete.

        return

DATA_DIR = "../../data"
KEY_FILE = os.path.abspath(f"{DATA_DIR}\data-analytics-engineer-minh-0b2185ce811c.json")
