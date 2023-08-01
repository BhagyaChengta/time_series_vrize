import json
import logging
from app.controller.aws.base import AWS
from app.models.modelling_models.models import PreprocessingColumnsMapping, HyperParameterTuneModel
from sagemaker.spark.processing import PySparkProcessor
import os
from datetime import date, datetime
from app.controller.aws.helpers import get_s3_path


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

class ParameterTuning:
    def __init__(self, data:HyperParameterTuneModel):
        self.session_id= data.session_id
        self.aws_role= data.aws_role
        self.s3_output_location=get_s3_path(data.session_id,"processed_data/train/")
        self.instance_type= data.instance_type
        self.instance_count= data.instance_count
        self.secretsManagerArn= data.secretsManagerArn
        self.d=(data.d)
        self.p=(data.p_range)
        self.q=(data.q_renge)
        self.script_path = None
        self.job_name = None
        config_file_path=os.path.join(os.getcwd(),'app','data',data.session_id,'config.json')
        self.config=json.loads(open(config_file_path).read())
        self.bucket_name=self.config["data_ingestion_model"]["s3_ouptut_location"]

    
    def upload_main_file(self):

        try:
            # s3://smarter-alerts-vrize/data/
            input_bucket_name = self.bucket_name.replace(
                "s3://", '').split('/')
            
            input_bucket_name = [x for x in input_bucket_name if x.strip() != '']
            folder = 'data_modelling_scripts/sagemaker_script_parameter_tuning.py'
            bucket_name = input_bucket_name[0]
            if len(input_bucket_name) > 1:
                folder = "/".join(input_bucket_name[1:]) + \
                    '/data_preprocessing_scripts/sagemaker_script_parameter_tuning.py'
            aws_session = AWS().get_boto3_session()

            s3 = aws_session.client('s3')

            result = s3.upload_file(
                Filename= os.path.join(os.getcwd(),'app','controller','modelling','sagemaker_script_parameter_tuning.py'),
                Bucket=bucket_name,
                Key=folder
                )  
            
            logging.info(f"Uploaded main file to S3. Result: {result}")

            self.script_path = "s3://{}/{}".format(bucket_name, folder)

            return {"status": "success"}
        
        except Exception as e:
            logging.error(f"Failed to upload main file to S3. Error: {str(e)}")
            return {"status": "failed", "error": str(e)}
        


    def run_pipeline(self):

        try:

            sage_session = AWS().get_sage_session()
            d=str(self.d)
            d=d[1:-1]
            p=str(self.p)
            p=p[1:-1]
            q=str(self.q)
            q=q[1:-1]

            spark_processor = PySparkProcessor(
                base_job_name="spark-hyperparameter",
                framework_version="3.1",
                role=self.aws_role,
                instance_count=self.instance_count,
                instance_type=self.instance_type,
                max_runtime_in_seconds=432000,
                sagemaker_session=sage_session
            )
            spark_processor.run(
            submit_app=self.script_path,
            arguments=[
                '--session_id', self.session_id,
                '--aws_role', self.aws_role,
                '--s3_output_location',self.s3_output_location,
                '--instance_type', self.instance_type,
                '--d',d,
                '--p',p,
                '--q',q
            ],
            wait=False
)


            output = spark_processor.jobs[-1].describe()
            processing_job_name = output['ProcessingJobName']

            if os.path.exists(os.path.join(os.getcwd(),'app','data',self.session_id,'runs'))==False:
                os.mkdir(os.path.join(os.getcwd(),'app','data',self.session_id,'runs'))

            with open(os.path.join(os.getcwd(),'app','data',self.session_id,'runs',f'{processing_job_name}.json'),'w') as f:
                f.write(json.dumps(output,default=json_serial, indent=4, separators=(',', ': ')))

            logging.info(f"Pipeline executed successfully. Processing Job Name: {processing_job_name}")

            self.job_name = processing_job_name

            return {"status": "success", "job_name": processing_job_name}
        except Exception as e:

            logging.error(f"Failed to execute pipeline. Error: {str(e)}")
            return {"status": "failed", "error": str(e)}
        
        

  
    


