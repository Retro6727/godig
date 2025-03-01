import boto3
import pymysql
import pandas as pd
import json
import os

s3 = boto3.client('s3')
glue = boto3.client('glue')


RDS_HOST = os.getenv("RDS_HOST")
RDS_USER = os.getenv("RDS_USER")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")
RDS_DB = os.getenv("RDS_DB")

def read_s3_data(bucket_name, file_key):
    obj = s3.get_object(Bucket = bucket_name, Key=file_key)
    data = obj['Body'].read().decode('utf-8')
    return pd.read_csv(pd.compat.StringIO(data))

def insert_into_rds(df):
    try:
        connection = pymysql.connect(host=RDS_HOST, user=RDS_USER, password=RDS_PASSWORD, database=RDS_DB)
        cursor = connection.cursor()

        for _, row in df.iterrows():
            sql = "INSERT INTO my_table (col1, col2) VALUES (%s, %s)"
            cursor.execute(sql, (row['col1'], row['col2']))

        connection.commit()
        cursor.close()
        connection.close()
        return True
    except Exception as e:
        print(f"RDS Insert failed: {e}")
        return False
    
def push_to_glue(df, database_name, table_name):
    try:
        glue.create_table(
            DatabaseName=database_name,
            TableInput={
                'Name': table_name,
                'StorageDescriptor': {
                    'Columns': [{'Name': col, 'Type': 'string'} for col in df.columns],
                    'Location': f"s3://{database_name}/{table_name}/"
                },
                'TableType': 'EXTERNAL_TABLE'
            }
        )
        return True
    except Exception as e:
        print(f"Glue Insert Failed: {e}")
        return False
    
def lambda_handler(event, context):
    bucket = event['bucket']
    key = event['key']

    df = read_s3_data(bucket, key)

    if not insert_into_rds(df):
        push_to_glue(df, "my_glue_db", "my_table")

    return {"statusCode": 200, "body": json.dumps("Data Processed Successfully")}