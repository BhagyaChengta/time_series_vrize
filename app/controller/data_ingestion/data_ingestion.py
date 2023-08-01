import json
import datetime
import shutil
import os
import csv
import logging
from app.models.models import DataIngestionInputModel,InputDbDetails, MySQLCreds
from app.controller.aws.helpers import upload_to_s3
from app.controller.aws.base import AWS
from pyspark.sql import SparkSession
from sagemaker.spark.processing import PySparkProcessor
from datetime import date, datetime

from app.controller.db.db import SnowflakeConnector,Dbbase, MySQLConnector

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))




class DataIngestion:
    def __init__(self, data: DataIngestionInputModel) -> None:
        self.data = data
        self.script_path = None
        self.job_name = None
        config_file_path=os.path.join(os.getcwd(),'app','data',data.session_id,'config.json')
        self.config=json.loads(open(config_file_path).read())

    def upload_main_file(self):

        try:
            
            # s3://smarter-alerts-vrize/data/
            input_bucket_name = self.data.s3_ouptut_location.replace(
                "s3://", '').split('/')
            input_bucket_name = [x for x in input_bucket_name if x.strip() != '']
            folder = 'data_ingestion_scripts/sagemaker_script.py'
            bucket_name = input_bucket_name[0]
            if len(input_bucket_name) > 1:
                folder = "/".join(input_bucket_name[1:]) + \
                    '/data_ingestion_scripts/sagemaker_script.py'

            aws_session = AWS().get_boto3_session()

            s3 = aws_session.client('s3')

            result = s3.upload_file(
                Filename= os.path.join(os.getcwd(),'app','controller','data_ingestion','sagemaker_script.py'),
                Bucket=bucket_name, 
                Key=folder
                )   
            
            print(result)
            self.script_path = "s3://{}/{}".format(bucket_name, folder)

            return {"status": "success"}
        except Exception as e:
            logging.error("Error occurred while uploading file to S3: %s", str(e))
            return {"status": "failed", "error": str(e)}
        

    def run_pipeline(self):
        try:
            query_strings = f'SELECT {",".join(self.data.column_names)} FROM {self.data.database}.{self.data.table_name};'
            sage_session = AWS().get_sage_session()

            spark_processor = PySparkProcessor(
                base_job_name="spark-data-ingestion",
                framework_version="3.1",
                role=self.data.aws_role,
                instance_count=self.data.instance_count,
                instance_type=self.data.instance_type,
                max_runtime_in_seconds=432000,
                sagemaker_session=sage_session
            )
            if self.config['dataset_source_type'] != self.data.dataset_source_type:
                logging.error("Dataset Source Type Mismatch")
                return {"status": "failed", "error": "Dataset Source Type Mismatch"}

            host = self.config['connection_params']['host']
            username = self.config['connection_params']['username']
            password = self.config['connection_params']['password']
            database = self.config['connection_params']['database']
            sessionid= self.config['session_id']
            query_string=query_strings
            s3_output_location = self.data.s3_ouptut_location + f'raw_data/{self.data.session_id}'

            

            spark_processor.run(
            submit_app=self.script_path,
            arguments=[
                '--datasetSourceType', str(self.data.dataset_source_type),
                '--host', str(host),
                '--session_id',str(sessionid),
                '--database', str(database),
                '--s3OutputLocation', str(s3_output_location),
                '--queryString', str(query_string),
                '--username', str(username),
                '--password', str(password)
            ],
            wait=False
        )


            output = spark_processor.jobs[-1].describe()
            processing_job_name = output['ProcessingJobName']

            runs_dir = os.path.join(os.getcwd(), 'app', 'data', self.data.session_id, 'runs')
            if not os.path.exists(runs_dir):
                os.makedirs(runs_dir)

            with open(os.path.join(os.getcwd(), 'app', 'data', self.data.session_id, 'runs', f'{processing_job_name}.json'), 'w') as f:
                f.write(json.dumps(output, default=json_serial, indent=4, separators=(',', ': ')))

            self.job_name = processing_job_name

            return {"status": "success", "job_name": processing_job_name}

        except Exception as e:
            logging.error("Error occurred while running the pipeline: %s", str(e))
            return {"status": "failed", "error": str(e)}
            










#     