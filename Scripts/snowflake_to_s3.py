import snowflake.connector
import boto3
from snowflake.connector import DictCursor
from botocore.exceptions import NoCredentialsError

# Snowflake connection parameters
snowflake_user = 'your_snowflake_user'
snowflake_password = 'your_snowflake_password'
snowflake_account = 'your_snowflake_account'
snowflake_warehouse = 'your_snowflake_warehouse'
snowflake_database = 'your_snowflake_database'
snowflake_schema = 'your_snowflake_schema'

# AWS S3 connection parameters
aws_access_key = 'your_aws_access_key'
aws_secret_key = 'your_aws_secret_key'
s3_bucket = 'your_s3_bucket'
s3_prefix = 'your_s3_prefix'

# Snowflake SQL query to fetch data
snowflake_query = 'SELECT * FROM your_snowflake_table'

# Function to execute Snowflake query and upload data to S3
def pull_data_from_snowflake_to_s3():
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema
    )

    # Create a cursor using a DictCursor
    cursor = conn.cursor(DictCursor)

    try:
        # Execute Snowflake query
        cursor.execute(snowflake_query)

        # Fetch the query result
        result = cursor.fetchall()

        # Upload data to S3
        upload_data_to_s3(result)

    finally:
        # Close Snowflake connection
        cursor.close()
        conn.close()

# Function to upload data to S3
def upload_data_to_s3(data):
    try:
        # Create an S3 client
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

        # Upload data to S3
        s3.put_object(Body=str(data), Bucket=s3_bucket, Key=f'{s3_prefix}/output_data.json')

        print("Data successfully uploaded to S3.")

    except NoCredentialsError:
        print("AWS credentials not available.")

# Execute the data pull and upload process
pull_data_from_snowflake_to_s3()
