import csv

import GoogleCloud as gg
import postgres as pg
import psycopg2
import psycopg2.extras
import os

#BUCKET_NAME = "data-abalytics-engineer"

WRITE_DISPOSITION = gg.bigquery.WriteDisposition.WRITE_APPEND
# Declare bucket_name to upload data
bucket_name = "data-abalytics-engineer"

uploader = gg.GoogleCloudHandler(gg.KEY_FILE, bucket_name)

def extract_postgres_to_gcs(table_name, bucket_name, filename_format):
    sql = f"SELECT * from {table_name}"
    dir_name = os.path.dirname(filename_format)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    csvfile = None
    try:
        records = []
        pg.CONNECTOR.execute(sql, True)
        field_names = []
        #column_names = ",".join(desc[0] for desc in pg.CONNECTOR.postgres_cursor.description) + "\n"
        for desc in pg.CONNECTOR.postgres_cursor.description:
            field_names.append(desc.name)

        i = 0
        file_counter = 1
        rows = []
        for row in pg.CONNECTOR.postgres_cursor.fetchall():
            if i == 0:
                file_id = str(file_counter).zfill(4)
                remote_path = filename_format.format(table_name = table_name, id=file_id)
                # = filename_format.format(id=file_id)
                csvfile = open(os.path.abspath(remote_path), mode="w", newline="\n")
                cvs_writer = csv.DictWriter(csvfile, field_names)
                cvs_writer.writeheader()
                 #csvfile.writelines(column_names)
            #csvfile.writelines( ", ".join(str(item) for item in row) + "\n")
            row_dict = dict(row)
            cvs_writer.writerow(row_dict)
            i += 1
            if i == 201:
                i = 0
                file_counter += 1
                finalize_file(csvfile, remote_path)

    except psycopg2.Error  as error:
        print("Failed to read data from table", error)

    finally:
        pg.CONNECTOR.close_connection()
        print("The connection is closed")
    if csvfile != None:
        finalize_file(csvfile, remote_path)
    return

#upload and delete file
def finalize_file(csvfile, remotefile):
    filename = csvfile.name
    csvfile.close()
    uploader.upload(filename, remotefile)

    os.remove(filename)
    return


table_name = "employees"
# Make variable filename_format here with contain table_name
filename_format = "data/{table_name}/{id}.csv"

# extract_postgres_to_gcs(
#     table_name=table_name,
#     bucket_name=bucket_name,
#     filename_format=filename_format
# )

# generate source_uirs variable base on bucket_name and file_name_format
#remote_path = filename_format.format(table_name = table_name)
source_uris = f"gs://{bucket_name}/{filename_format.format(table_name = table_name, id='*')}"

# Declare dataset
dataset = "sql_practice"
# Name of table in bigquery to load data into
bigquery_table_name = "employees2"
schema = {"gender": "STRING"}

# declare write_disposition
bigquery_write_disposition = gg.bigquery.WriteDisposition.WRITE_APPEND

uploader.load_to_bigquery(
    source_uris=source_uris,
    dataset=dataset,
    table_name=bigquery_table_name,
    write_disposition=bigquery_write_disposition,
    schema = None
)