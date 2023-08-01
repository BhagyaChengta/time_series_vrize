import io
import json
import logging
import boto3
import pandas as pd
from app.controller.aws.base import AWS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataInformation:
    def __init__(self, s3_path:str,session_id:str):
        self.s3_path = s3_path
        self.session_id = session_id

    def get_sample_data(self):
        try:
           aws_session = AWS().get_boto3_session()
           s3 = aws_session.resource('s3')
           s3_bucket=self.s3_path.replace("s3://", '').split('/')
           s3_bucket=[x for x in s3_bucket if x.strip() != ''][0]
           bucket = s3.Bucket(s3_bucket)
           obj = bucket.Object(f'raw_data/{self.session_id}/config/data_info.json')
           body = obj.get()['Body'].read().decode('utf-8')
           config = json.loads(body)
           logging.info(f"Fetched sample data from S3 bucket: {s3_bucket}, Session ID: {self.session_id}")

           return config
        except Exception as e:
            logging.error("Error occurred while fetching sample data from S3: %s", str(e))
            return None