import json
from app.controller.aws.base import AWS
from app.models.modelling_models.models import PreprocessingColumnsMapping, retraining_model_arg
from sagemaker.spark.processing import PySparkProcessor
import os
from datetime import date, datetime
from app.models.models import DataIngestionInputModel
from app.models.preprocessing_model.models import ApplyPreprocessing
from app.controller.data_ingestion.data_ingestion import DataIngestion
from app.controller.data_preprocessing.data_preprocess import DataPreprocess


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


class CustomJSONEncoder(json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, PreprocessingColumnsMapping):
            return obj.dict()
        return super().default(obj)


class RetrainingModel:
    def __init__(self, data: retraining_model_arg) -> None:

        self.session_id = data.session_id
        self.start_date = data.start_date
        self.end_date = data.end_date

        self.script_path = None
        self.job_name = None

        config_file_path = os.path.join(os.getcwd(), 'app', 'data', self.session_id, 'config.json')
        with open(config_file_path, "r") as f:
            self.config = json.load(f)

    def upload_main_file(self, ingestion_input, preprocess_input):
        try:
            input_bucket_name = preprocess_input["s3_output_location"].replace("s3://", "").split("/")
            input_bucket_name = [x for x in input_bucket_name if x.strip() != ""]
            folder = "data_retraining_scripts/sagemaker_script.py"
            bucket_name = input_bucket_name[0]
            if len(input_bucket_name) > 1:
                folder = "/".join(input_bucket_name[1:]) + "/data_retraining_scripts/sagemaker_script.py"

            aws_session = AWS().get_boto3_session()

            s3 = aws_session.client("s3")

            result = s3.upload_file(
                Filename=os.path.join(os.getcwd(), "app", "controller", "modelling", "sagemaker_script_retraining_model.py"),
                Bucket=bucket_name,
                Key=folder,
            )

            print(result)
            self.script_path = f"s3://{bucket_name}/{folder}"

            return {"status": "success"}
        except Exception as e:
            return {"status": "failed", "error": str(e)}

    def run_pipeline(self, ingestion_input, preprocess_input):
        try:
            query_strings = f'SELECT {",".join(ingestion_input["column_names"])} FROM {ingestion_input["database"]}.{ingestion_input["table_name"]} WHERE {ingestion_input["column_names"][0]} >= "{self.start_date}" AND {ingestion_input["column_names"][0]} <= "{self.end_date}";'
            sage_session = AWS().get_sage_session()

            spark_processor = PySparkProcessor(
                base_job_name="spark-retraining",
                framework_version="3.1",
                role=preprocess_input["aws_role"],
                instance_count=preprocess_input["instance_count"],
                instance_type=preprocess_input["instance_type"],
                max_runtime_in_seconds=432000,
                sagemaker_session=sage_session,
            )

            s3_output_location = preprocess_input["s3_output_location"] + f"raw_data/retraining_data/{self.session_id}"

            input_data_s3_path = preprocess_input["s3_output_location"] + f"raw_data/retraining_data/" + self.session_id + "/"
            encoder = CustomJSONEncoder()

            spark_processor.run(
                submit_app=self.script_path,
                arguments=[
                    "--datasetSourceType",
                    str(self.config["dataset_source_type"]),
                    "--host",
                    str(self.config["connection_params"]["host"]),
                    "--session_id",
                    str(self.session_id),
                    "--database",
                    str(self.config["connection_params"]["database"]),
                    "--s3OutputLocation",
                    str(s3_output_location),
                    "--queryString",
                    str(query_strings),
                    "--username",
                    str(self.config["connection_params"]["username"]),
                    "--password",
                    str(self.config["connection_params"]["password"]),
                    "--input_data_s3_path",
                    input_data_s3_path,
                    "--train_test_split_ratio",
                    preprocess_input["train_test_split_ratio"],
                    "--preprocessing_column_mapping",
                    json.dumps(preprocess_input["preprocessing_mapping"], default=encoder.default),
                ],
                wait=False,
            )

            output = spark_processor.jobs[-1].describe()
            processing_job_name = output["ProcessingJobName"]
            self.job_name = processing_job_name

            return {"status": "success", "job_name": processing_job_name}
        except Exception as e:
            return {"status": "failed", "error": str(e)}