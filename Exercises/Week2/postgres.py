import csv
import os

import psycopg2


class Field:
    NUMERIC_TYPES = ("serial", "smallint", "integer", "bigint", "decimal", "real", "double precision")

    def __init__(self, field_string):
        self.name, self.type = field_string.split(" ", 1)
        return

    def __repr__(self):
        return f"{self.name} {self.type}"

    def quote_value(self, value):
        if any(numeric in self.type.lower() for numeric in self.NUMERIC_TYPES):
            return value
        else:
            return f"'{value}'"


class PostgreTable:
    def __init__(self, table_name, column_string):
        self.__table_name = table_name
        self.__columns = [Field(column) for column in column_string]
        return

    def __create_table(self):
        CONNECTOR.execute(f"Drop Table if exists {self.__table_name}")
        fields = ", ".join(str(field) for field in self.__columns)
        create_table_sql = f"""
           CREATE TABLE IF NOT EXISTS {self.__table_name}(
               {fields}
           )
        """
        print(create_table_sql)
        CONNECTOR.execute(create_table_sql, True)
        return

    def export_all_to_dict(self):
        res :int
        return res

    def import_csv(self, CSV_path):
        sql = self.__readcsv(CSV_path)
        if len(sql) < 1:
            print("Cannot build SQl from CSV")
            return
        self.__create_table()
        #print(sql)
        CONNECTOR.execute(sql)
        return

    # @return: sql string to insert table
    def __readcsv(self, CSV_path):
        sql = f"insert into {self.__table_name} "
        CSV_path = os.path.abspath(f"{DATA_DIR}\{CSV_path}")

        with open(CSV_path, newline="", mode="r") as csvfile:
            reader = csv.reader(csvfile)
            inserts = []
            i = 0
            for row in reader:
                if (i == 0):
                    temp_list = list()
                    for item in row:
                        temp_list.append([x for x in self.__columns if x.name == item ][0])
                    self.__columns = temp_list
                    col_string = ",".join(field.name.strip() for field in self.__columns)
                    sql = f"{sql} ({col_string}) values"
                    i = 1
                else:
                    items = ""
                    for i in range (len(row)):
                        items = items + self.__columns[i].quote_value(row[i])
                        if i < len(row) - 1: items += ", "
                    inserts.append("(" + items +")")
            sql += ", ".join(inserts)
        csvfile.close()
        #print(sql)
        return sql + ";"

class PostgreConnection:
    def __init__(self, __connection_params, database):
        self.__connection_params = __connection_params
        self.__database = database
        self.__postgres_connection = None
        self.postgres_cursor = None
        return

    def __connect(self):
        params = self.__connection_params
        self.__postgres_connection = psycopg2.connect(
            #            database = self.__database, user='postgres', password='postgres', host='127.0.0.1', port='5432'
            database=self.__database, user=params['user'], password=params['password'],
            host=params['host'], port=params['port']
        )
        self.__postgres_connection.autocommit = True

        # Creating a cursor object using the cursor() method
        self.postgres_cursor = self.__postgres_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        #self.postgres_cursor = self.__postgres_connection.cursor()
        return

    def execute(self, sql, keep_connection=True):
        if self.__postgres_connection == None:
            self.__connect()
        if self.__postgres_connection == None:
            print("Cannot connect to db with those details: ", self.__database, self.__connection_params)
            return

        self.postgres_cursor.execute(sql)
        if not keep_connection:
            self.postgres_cursor.close()
            self.__postgres_connection.close()
        return

    def close_connection(self):
        if self.__postgres_connection:
            self.__postgres_connection.close()
        if self.postgres_cursor:
            self.postgres_cursor.close()
        return

DATA_DIR = "../../data"
CONNECTION_PARAMS = {'user': 'postgres', 'password': 'postgres', 'host': '127.0.0.1', 'port': '5432'}
DATABASE = "data-analytics-engineer"
CONNECTOR = PostgreConnection(CONNECTION_PARAMS, DATABASE)
JSON_FILE = "alltables.json"
tables = {}

def load_from_json():
    import json
    with open(os.path.abspath(f"{DATA_DIR}\{JSON_FILE}")) as jsonfile:
        json_obj= json.loads(jsonfile.read())
        for table in json_obj:
            new_table = PostgreTable(table['name'], table['fields'])
            tables.append(new_table)
    jsonfile.close()
    return