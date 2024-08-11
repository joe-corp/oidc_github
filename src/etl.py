import boto3
import pandas as pd
import logging
import os
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from datetime import datetime, timezone
import psycopg2

class DynamoDBConnector:
    """Handles connections and operations related to DynamoDB."""
    
    def __init__(self):
        self.client = boto3.client('dynamodb')

    def list_tables(self):
        """List all DynamoDB tables."""
        try:
            return self.client.list_tables()['TableNames']
        except NoCredentialsError:
            logging.error("AWS credentials not found.")
            raise
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            raise

class S3Operations:
    """Manage S3 operations for storing and retrieving data."""
    
    def __init__(self):
        self.client = boto3.client('s3')

    def upload_file(self, file_name, bucket):
        """Upload a file to an S3 bucket."""
        try:
            self.client.upload_file(file_name, bucket, file_name)
            logging.info(f"Uploaded {file_name} to {bucket}")
        except Exception as e:
            logging.error(f"Failed to upload {file_name} to {bucket}: {e}")
            raise

class DataExtraction:
    """Extracts data from DynamoDB, saves locally, uploads to S3, and copies to Redshift."""
    
    def __init__(self, redshift_details, arn, environment):
        self.dynamo = DynamoDBConnector()
        self.s3 = S3Operations()
        self.redshift = RedshiftConnector(**redshift_details)
        self.local_storage_dir = 'raw'
        self.arn = arn
        self.environment = environment

    def extract_and_load(self, bucket_name):
        """Main method to extract and load data."""
        print(bucket_name)
        try:
            tables = self.dynamo.list_tables()
            for table in tables:
                if self.environment == 'dev' and not table.startswith('stg'):
                    continue
                logging.info(f"starting {table}")
                df = self.pull_data_from_dynamo(table)
                if not os.path.exists(self.local_storage_dir):
                    os.makedirs(self.local_storage_dir)
                local_path = f"{self.local_storage_dir}/{table}.csv"
                df.to_csv(local_path, index=False)
                logging.info(f"uploading {table} to s3")
                self.s3.upload_file(local_path, bucket_name)
                logging.info(f"copying {table} to redshift")
                print(f"s3://{bucket_name}/{local_path}")
                self.redshift.copy_s3_data(f"s3://{bucket_name}/{local_path}", f"raw_{table}", self.arn)
        except Exception as e:
            logging.error(f"Error during data extraction and loading: {e}")
        
    def transform_data(self, items, pk):
        """Transform data to the specified format."""
        transformed_data = []
        for item in items:
            now = datetime.now()
            utc_now = now.astimezone(timezone.utc)
            formatted_time = utc_now.strftime("%Y-%m-%d %H:%M:%S%Z")
            transformed_item = {
                'row_id': item[pk],
                'data': item,
                'created': item.get('created', None),
                'updated': formatted_time ,
                'isDeleted': False
            }
            transformed_data.append(transformed_item)
        return transformed_data
    
    def pull_data_from_dynamo(self, table_name):
        """Scan and pull all data from a DynamoDB table."""
        
        dynamo_resource = boto3.resource('dynamodb')
        table = dynamo_resource.Table(table_name)
        for keys in table.key_schema:
            if keys.get('KeyType',None) == 'HASH':
                pk = keys['AttributeName']
        
        response = table.scan()
        data = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])
        transformed_data = self.transform_data(data,pk)
        return pd.DataFrame(transformed_data)


class RedshiftConnector:
    def __init__(self, host, dbname, user, password, port=5439):
        self.connection = psycopg2.connect(host=host,
                                                dbname=dbname,
                                                user=user,
                                                password=password,
                                                port=port)

  
    def _execute_query(self, query):
        """Execute a query and return the results."""
        conn = None
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                if cursor.description:  
                    result = cursor.fetchall()
                else:
                    result = None
                self.connection.commit()  
                return result
        except Exception as e:
            
            self.connection.rollback()
            print(f"Error executing query: {e}")
          

    def close(self):
        """Close all connections in the pool."""
        self.connection.close()
    
    def create_table_if_exist(self,table_name):
        query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
               id varchar,
               data varchar(max),
               createdAt datetime,
               updatedAt datetime,
               isDeleted boolean
                )
               """

        self._execute_query(query)
    
    def truncate_table(self,table_name):
        query = f"""
                TRUNCATE TABLE {table_name}
                """
        self._execute_query(query)
        
    def copy_s3_data(self, s3_path, table_name, role_arn):

        self.create_table_if_exist(table_name)
        self.truncate_table(table_name)
        query = f"""
                COPY {table_name} FROM '{s3_path}'
                IAM_ROLE '{role_arn}'
                delimiter ','
                ignoreheader 1
                csv quote as '"'
                """
        self._execute_query(query)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    redshift_details = {'dbname' : os.getenv('dbname'),
                        'host' : os.getenv('host'),
                        'port' :5439,
                        'user' : os.getenv('user'),
                        'password' : os.getenv('password')}
    print(redshift_details)
    environment = os.getenv('env','dev')
    arn = os.getenv('redshift_copy_arn')
    bucket = os.getenv('data_bucket')
    print(bucket)
    extraction = DataExtraction(redshift_details, arn, environment)
    extraction.extract_and_load(bucket)
