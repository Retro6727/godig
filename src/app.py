import boto3
import os
import psycopg2

def handler(event, context):
    s3_bucket = os.environ.get('S3_BUCKET')
    s3_key = os.environ.get('S3_KEY')
    rds_host = os.environ.get('RDS_HOST')
    rds_user = os.environ.get('RDS_USER')
    rds_password = os.environ.get('RDS_PASSWORD')
    rds_database = os.environ.get('RDS_DATABASE')
    
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        data = response['Body'].read().decode('utf-8')
        lines = data.splitlines()
        try:
            conn = psycopg2.connect(host=rds_host, user=rds_user, password=rds_password, database=rds_database)
            cur = conn.cursor()
            for line in lines:
                values = line.split(",")
                if len(values) == 2:
                    cur.execute("INSERT INTO your_table (column1, column2) VALUES (%s, %s)", (values[0], values[1]))
            conn.commit()
            cur.close()
            conn.close()
            print("Data pushed to RDS successfully.")
        except Exception as rds_error:
            print(f"Failed to push to RDS: {rds_error}")
            print("Glue process must be triggered outside of this lambda function.")
    except Exception as s3_error:
        print(f"Failed to retrieve data from S3: {s3_error}")
    return {
        'statusCode': 200,
        'body': 'Function executed.'
    }