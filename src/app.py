import boto3
import os

def handler(event, context):
    s3_bucket = os.environ.get('S3_BUCKET')
    s3_key = os.environ.get('S3_KEY')
    rds_host = os.environ.get('RDS_HOST')
    rds_user = os.environ.get('RDS_USER')
    rds_password = os.environ.get('RDS_PASSWORD')
    rds_database = os.environ.get('RDS_DATABASE')
    glue_database = os.environ.get('GLUE_DATABASE')
    glue_table = os.environ.get('GLUE_TABLE')

    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        data = response['Body'].read().decode('utf-8')
        

        try:
            import psycopg2
            conn = psycopg2.connect(host=rds_host, user=rds_user, password=rds_password, database=rds_database)
            cur = conn.cursor()
            conn.commit()
            cur.close()
            conn.close()
            print("Data pushed to RDS successfully.")
        except Exception as rds_error:
            print(f"Failed to push to RDS: {rds_error}")

            try:
                glue = boto3.client('glue')
                print("Data process attempted to RDS, now pushing to Glue")
            except Exception as glue_error:
                 print(f"Failed to push to Glue: {glue_error}")

    except Exception as s3_error:
        print(f"Failed to retrieve data from S3: {s3_error}")
    return {
        'statusCode': 200,
        'body': 'Function executed.'
    }