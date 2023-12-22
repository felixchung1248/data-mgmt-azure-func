import logging

import azure.functions as func
import pyodbc
import json
import os
import time
import requests

# Retrieve connection details from environment variables
server = os.environ.get('AZURE_SQL_SERVER')
database = os.environ.get('AZURE_SQL_DATABASE')
username = os.environ.get('AZURE_SQL_USER')
password = os.environ.get('AZURE_SQL_PASSWORD')
jiraUrl = os.environ.get('JIRA_URL')

def submitJira(jsonData,url):
    # Define the headers, including the content type and any necessary authentication tokens
    headers = {
        'Content-Type': 'application/json',
        # 'Authorization': 'Bearer your_auth_token'  # Uncomment and replace if authentication is required
    }

    # Send the POST request
    response = requests.post(url, headers=headers,
                         data=json.dumps(jsonData))
    



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

    datasetPath = json_array[0].get('dataset_path')


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
                cursor.execute(f"select count(1) as cnt from dbo.DATA_CATALOG_DRAFT where status='pending' and dataset_path = '{datasetPath}'")
                # Fetch all the rows
                rows = cursor.fetchall()
                for row in rows:
                    if row[0] > 0:
                        responseText = "There has already been pending request for this dataset for approval"
                        return func.HttpResponse(responseText)
                    
                # Iterate over the JSON array and insert each item
                batch_key = int(round(time.time() * 1000))
                for item in json_array:
                    item['batch_key'] = batch_key

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


                # Define the headers, including the content type and any necessary authentication tokens
                headers = {
                    'Content-Type': 'application/json',
                        # 'Authorization': 'Bearer your_auth_token'  # Uncomment and replace if authentication is required
                }

                # Send the POST request
                response = requests.post(jiraUrl, headers=headers,data=json.dumps(json_array))
                
                if response.status_code == 200 and response.text == 'Jira submitted successfully.':
                     # Commit the transaction
                    conn.commit()
                    return func.HttpResponse("Data catalog draft can be imported successfully")
                else:
                    conn.rollback()
                    return func.HttpResponse('Get some issue when submitting Jira ticket', status_code=9000)
    

            except pyodbc.DatabaseError as e:
                conn.rollback()  # Rollback the transaction on error
                logging.error(f"Database error occurred: {str(e)}")
                # return func.HttpResponse(str(e), status_code=9001)

    
