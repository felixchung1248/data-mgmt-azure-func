import logging

import azure.functions as func
import pyodbc
import json
import os
import time

# Retrieve connection details from environment variables
server = os.environ.get('AZURE_SQL_SERVER')
database = os.environ.get('AZURE_SQL_DATABASE')
username = os.environ.get('AZURE_SQL_USER')
password = os.environ.get('AZURE_SQL_PASSWORD')

def main(req: func.HttpRequest) -> func.HttpResponse:
    # Get the JSON from the POST request body
    try:
        json_array = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)
    
    connection_string = """
        Driver={{ODBC Driver 18 for SQL Server}};
        Server={server};
        Database={database};
        Uid={username};
        Pwd={password};
        Encrypt=yes;
        TrustServerCertificate=no;
        Connection Timeout=30;
    """.format(server=server, database=database, username=username, password=password)
    print(connection_string)
#     # Your JSON data
#     json_array = [
#   {
#     "name": "UsageDate",
#     "description": "aaa",
#     "is_sensitive": 0,
#     "data_type": "DATE",
#     "dataset_path": "demo-catalog-01/Marketing/01 Curated/cost-analysis",
#     "status": "Pending",
#     "create_datetime": "2023-12-20T08:23:26.608Z",
#     "create_user": "frontend",
#     "last_modified_datetime": "2023-12-20T08:23:26.608Z",
#     "last_modified_user": "frontend"
#   },
#   {
#     "name": "CostUSD",
#     "description": "bbb",
#     "is_sensitive": 1,
#     "data_type": "DOUBLE",
#     "dataset_path": "demo-catalog-01/Marketing/01 Curated/cost-analysis",
#     "status": "Pending",
#     "create_datetime": "2023-12-20T08:23:26.608Z",
#     "create_user": "frontend",
#     "last_modified_datetime": "2023-12-20T08:23:26.608Z",
#     "last_modified_user": "frontend"
#   },
#   {
#     "name": "Cost",
#     "description": "",
#     "is_sensitive": 0,
#     "data_type": "DOUBLE",
#     "dataset_path": "demo-catalog-01/Marketing/01 Curated/cost-analysis",
#     "status": "Pending",
#     "create_datetime": "2023-12-20T08:23:26.608Z",
#     "create_user": "frontend",
#     "last_modified_datetime": "2023-12-20T08:23:26.608Z",
#     "last_modified_user": "frontend"
#   },
#   {
#     "name": "ForecastCost",
#     "description": "",
#     "is_sensitive": 0,
#     "data_type": "DOUBLE",
#     "dataset_path": "demo-catalog-01/Marketing/01 Curated/cost-analysis",
#     "status": "Pending",
#     "create_datetime": "2023-12-20T08:23:26.608Z",
#     "create_user": "frontend",
#     "last_modified_datetime": "2023-12-20T08:23:26.608Z",
#     "last_modified_user": "frontend"
#   },
#   {
#     "name": "Currency",
#     "description": "",
#     "is_sensitive": 0,
#     "data_type": "VARCHAR",
#     "dataset_path": "demo-catalog-01/Marketing/01 Curated/cost-analysis",
#     "status": "Pending",
#     "create_datetime": "2023-12-20T08:23:26.608Z",
#     "create_user": "frontend",
#     "last_modified_datetime": "2023-12-20T08:23:26.608Z",
#     "last_modified_user": "frontend"
#   }
# ]


    # SQL query to insert JSON string into the table
    sql_query = """
            INSERT INTO dbo.DATA_CATALOG_DRAFT 
            (
                batch_key
                ,name
                ,description
                ,is_sensitive
                ,data_type
                ,dataset_path
                ,status
                ,create_datetime
                ,create_user
                ,last_modified_datetime
                ,last_modified_user
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """

    # Establish a connection to the database
    with pyodbc.connect(connection_string) as conn:
        with conn.cursor() as cursor:
            try:
                # Iterate over the JSON array and insert each item
                batch_key = int(round(time.time() * 1000))
                for item in json_array:
                    # Convert each JSON object to a string
                    # json_string = json.dumps(item)

                    data_tuple = (
                    batch_key,
                    item['name'],
                    item['description'],
                    item['is_sensitive'],
                    item['data_type'],
                    item['dataset_path'],
                    item['status'],
                    item['create_datetime'],
                    item['create_user'],
                    item['last_modified_datetime'],
                    item['last_modified_user']
                    )
                
                    # Execute the SQL query
                    cursor.execute(sql_query, data_tuple)

                # Commit the transaction
                conn.commit()
                return func.HttpResponse("JSON data inserted successfully.")
            except pyodbc.DatabaseError as e:
                conn.rollback()  # Rollback the transaction on error
                return func.HttpResponse("Error inserting JSON data:", e)
    
