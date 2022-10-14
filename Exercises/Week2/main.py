import csv

import GoogleCloud as gg
import postgres as pg
import psycopg2
import psycopg2.extras
import os

BUCKET_NAME = "data-abalytics-engineer"
uploader = gg.GoogleCloudHandler(gg.KEY_FILE, BUCKET_NAME)
REMOTE_DIR = "output"
TABLE_ID = "sql_practice.employees1"

def table_to_cvs():
    sql = "SELECT * from employees"
    dir_name = "output"
    file_name = "employees"
    schema = {"gender": "STRING"}
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

        print(field_names)
        i = 0
        file_counter = 1
        rows = []
        for row in pg.CONNECTOR.postgres_cursor.fetchall():
            if i == 0:
                csvfile = open(os.path.abspath(f"{dir_name}\{file_name}{str(file_counter).zfill(5)}.csv"), mode="w", newline="\n")
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
                finalize_file(csvfile, schema)

    except psycopg2.Error  as error:
        print("Failed to read data from table", error)

    finally:
        pg.CONNECTOR.close_connection()
        print("The connection is closed")
    if csvfile != None:
        finalize_file(csvfile)
    return

#upload and delete file
def finalize_file(csvfile, schema):
    filename = csvfile.name
    csvfile.close()
    uploader.upload(filename, f"{REMOTE_DIR}/")
    gsutil = f"gs://{BUCKET_NAME}/{REMOTE_DIR}/{os.path.basename(filename)}"
    uploader.gsutil_to_bigquery(gsutil, TABLE_ID, schema)
    os.remove(filename)
    return

table_to_cvs()
#uploader.gsutil_to_bigquery("gs://data-abalytics-engineer/output/employees00001.csv", TABLE_ID)
