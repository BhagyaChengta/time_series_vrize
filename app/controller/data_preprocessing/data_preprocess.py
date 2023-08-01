import json
from app.controller.aws.base import AWS
import logging

from app.models.preprocessing_model.models import PreprocessingColumnsMapping, ApplyPreprocessing

from sagemaker.spark.processing import PySparkProcessor
import os
from datetime import date, datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CustomJSONEncoder(json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, PreprocessingColumnsMapping):
            return obj.dict()
        return super().default(obj)
    
class DataPreprocess:

    def __init__(self, data:ApplyPreprocessing):
        self.output_data_s3_path = data.s3_output_location
        self.session_id = data.session_id
        self.process_mapping = data.preprocessing_mapping
        self.test_train=data.train_test_split_ratio
        self.aws_role = data.aws_role
        self.instance_count = data.instance_count
        self.instance_type = data.instance_type
        self.script_path = None
        self.job_name = None
        config_file_path=os.path.join(os.getcwd(),'app','data',data.session_id,'config.json')
        self.config=json.loads(open(config_file_path).read())


    def save_preprocess_mapping(self,bucket_name:str):
        encoder = CustomJSONEncoder()
        output=[]
        for preprocess_type,column in self.process_mapping:
            if preprocess_type=='normalization':
                model_path = f"s3://{bucket_name}/preprocessing/min_max_scaler_{column}"
            elif preprocess_type=='standardization':
                model_path = f"s3://{bucket_name}/preprocessing/standard_scaler_{column}"    
            
            output.append({"preprocess_type":preprocess_type,"column":column,"model_path":model_path})

        if len(output)>0:
            with open(os.path.join(os.getcwd(),'app','data',self.session_id,'preprocess_models_path.json'), 'w') as outfile:
                json.dump(output, outfile,default=encoder.default)


    
    
    def upload_main_file(self):

        try:
            # s3://smarter-alerts-vrize/data/
            input_bucket_name = self.output_data_s3_path.replace(
                "s3://", '').split('/')
            input_bucket_name = [x for x in input_bucket_name if x.strip() != '']
            folder = 'data_preprocessing_scripts/sagemaker_script.py'
            bucket_name = input_bucket_name[0]
            if len(input_bucket_name) > 1:
                folder = "/".join(input_bucket_name[1:]) + \
                    '/data_preprocessing_scripts/sagemaker_script.py'

            aws_session = AWS().get_boto3_session()

            s3 = aws_session.client('s3')

            result = s3.upload_file(
                Filename= os.path.join(os.getcwd(),'app','controller','data_preprocessing','sagemaker_script.py'),
                Bucket=bucket_name,
                Key=folder
                )  
            
            logging.info(f"Uploaded main file to S3 bucket: {bucket_name}, Folder: {folder}")

            self.script_path = "s3://{}/{}".format(bucket_name, folder)

            return {"status": "success"}
        
        except Exception as e:
            logging.error("Error occurred while uploading main file to S3: %s", str(e))
            return {"status": "failed", "error": str(e)}
        



    def run_pipeline(self):

        try:

            input_data_s3_path=self.output_data_s3_path+"/raw_data/"+self.session_id+"/"
            encoder = CustomJSONEncoder()
            sage_session = AWS().get_sage_session()


            spark_processor = PySparkProcessor(
                base_job_name="spark-preprocessing",
                framework_version="3.1",
                role=self.aws_role,
                instance_count=self.instance_count,
                instance_type=self.instance_type,
                max_runtime_in_seconds=432000,
                sagemaker_session=sage_session
            )
            spark_processor.run(
                submit_app=self.script_path,
                arguments=['--input_data_s3_path', input_data_s3_path,'--output_data_s3_path', self.output_data_s3_path,
                           '--preprocessing_column_mapping', json.dumps(self.process_mapping, default=encoder.default),'--train_test_split_ratio',self.test_train],
                wait=False
            )


            output = spark_processor.jobs[-1].describe()
            processing_job_name = output['ProcessingJobName']

            if os.path.exists(os.path.join(os.getcwd(),'app','data',self.session_id,'runs'))==False:
                os.mkdir(os.path.join(os.getcwd(),'app','data',self.session_id,'runs'))

            with open(os.path.join(os.getcwd(),'app','data',self.session_id,'runs',f'{processing_job_name}.json'),'w') as f:
                f.write(json.dumps(output,default=encoder.default, indent=4, separators=(',', ': ')))


            
            self.job_name = processing_job_name

            return {"status": "success", "job_name": processing_job_name}

        except Exception as e:
            logging.error("Error occurred while running pipeline: %s", str(e))
            return {"status": "failed", "error": str(e)}
        return {"status": "success"}

  
    

