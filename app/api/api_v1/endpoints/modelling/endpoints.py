import asyncio
import json
import os
from typing import Any, Optional
import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.models.modelling_models.models import AvailableMLModels_Response, General_Response,TransformedDataDetails,TrainModel,HyperParameterTuneModel,retraining_model_arg,model_arg
from app.controller.db.db import SnowflakeConnector
from app.controller.data_ingestion.data_ingestion import DataIngestion
from app.controller.modelling.model_training import Modelling
from app.controller.modelling.parameter_tuning import ParameterTuning
from app.controller.modelling.retraining_model import RetrainingModel
from app.controller.modelling.model_arguments import model_arguments
from pydantic import ValidationError
import io
import boto3
import pandas as pd
import boto3
from app.core.config import settings
import numpy as np
router = APIRouter()
AVAILABLE_ML_MODELS = ["arima"]




@router.get("/available-models", status_code=200, response_model=AvailableMLModels_Response)
def get_available_dbs_connections() -> Any:
    return {"result": AVAILABLE_ML_MODELS}



@router.post("/ingested-data-info", status_code=200)
def ingested_data_information( *, data : TransformedDataDetails) -> Any :

    try:
        # Read the content of the config.json file
        config_file_path = os.path.join(os.getcwd(), 'app', 'data', data.session_id, 'config.json')
        with open(config_file_path, 'r') as f:
            config_data = json.load(f)
        # Extract the necessary information from the config data
        dataset_source_type = config_data.get('dataset_source_type', '')
        connection_params = config_data.get('connection_params', {})
        data_preprocessing_model = config_data.get('data_preprocessing_model', {})
        response = {
            'dataset_source_type': dataset_source_type,
            'connection_params': connection_params,
            'data_preprocessing_model': data_preprocessing_model
        }
        return response
    except Exception as e:
        return {'status': 'failed', 'error': str(e)}




@router.post("/model-arguments", status_code=200)
def data_ingestion(data: model_arg):
    try:
        obj=model_arguments(data)
        original_array = obj.generate_arguments()
        first_array = [original_array[0]]
        second_array = original_array[1][:]
        third_array = original_array[2][:]

        #return ("D = "+ first_array + "P_rage = "+ second_array +"Q_range="+third_array)
        return("D = ",first_array,"P_rage = ",second_array,"Q_range=",third_array)

    except Exception as e:

        print(e)

        return {'status': 'failed', 'error': str(e)}





@router.post("/hyper-parameter-tune", status_code=200)

def data_ingestion(data: HyperParameterTuneModel):
    try:
        obj=ParameterTuning(data)
        file_upload_result = obj.upload_main_file()
        if file_upload_result['status']=='success':
            pipeline_run_status = obj.run_pipeline()
            print(pipeline_run_status)
        return pipeline_run_status
    except Exception as e:
        print(e)
        return {'status': 'failed', 'error': str(e)}
hyper_parameters = ["pqr"]


@router.post("/best-hyperparameters")
def best_hyperparameters(*, data : TransformedDataDetails) -> Any :
    try:
        # Connect to S3
        s3 = boto3.client('s3', aws_access_key_id=settings.AWS_ACCESS_KEY, aws_secret_access_key=settings.AWS_SECRET_KEY)
        # Specify the bucket and file name
        bucket_name = 'time-series-data-ingested'
        file_key = 'parameter_tuning/'+data.session_id+'/data.json'
        # Retrieve the file from S3
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        data = obj['Body'].read().decode('utf-8')
        # Parse CSV data into a pandas DataFrame
        df = pd.read_csv(io.StringIO(data))
        keys = list(df.keys())
        # Assign the keys to separate variables
        mape = keys[0].strip("(")
        p = keys[1].strip(" (")
        d = keys[2].strip()
        q = keys[3].strip(")").strip()
        # Return the DataFrame
        output_list = ("MAPE",mape,"P",p,"D",d,"Q",q)

        formatted_output = {}
        for i in range(0, len(output_list), 2):
            key = output_list[i]
            value = output_list[i + 1]
            formatted_output[key] = float(value) if value.replace('.', '', 1).isdigit() else value

        return formatted_output
    



    except Exception as e:
        print(e)
        return {'status': 'failed', 'error': str(e)}
    



@router.post("/train-model", status_code=200) 
def train_model(data: TrainModel) -> Any :
    try:
        obj=Modelling(data)
        file_upload_result = obj.upload_main_file()
        if file_upload_result['status']=='success':
            pipeline_run_status = obj.run_pipeline()
            print(pipeline_run_status)
            config_file_path=os.path.join(os.getcwd(),'app','data',data.session_id,'config.json')
            config = json.loads(open(config_file_path).read())
            config['data_train_model'] = data.dict()
            with open(config_file_path, 'w') as f:
                json.dump(config, f, indent=4, separators=(',', ': '))
        return pipeline_run_status
    except Exception as e:
        print(e)
        return {'status': 'failed', 'error': str(e)}

    
@router.post("/model-retraining", status_code=200)
def data_ingestion(data: retraining_model_arg):
    try:
        obj = RetrainingModel(data)

        config_file_path = os.path.join(os.getcwd(), "app", "data", data.session_id, "config.json")
        with open(config_file_path, "r") as f:
            config_data = json.load(f)

        data_ingestion_input = {
            "database": config_data["data_ingestion_model"]["database"],
            "table_name": config_data["data_ingestion_model"]["table_name"],
            "column_names": config_data["data_ingestion_model"]["column_names"],
            "session_id": config_data["data_ingestion_model"]["session_id"],
            "dataset_source_type": config_data["data_ingestion_model"]["dataset_source_type"],
            "aws_role": config_data["data_ingestion_model"]["aws_role"],
        }

        data_preprocessing_input = {
            "session_id": config_data["data_preprocessing_model"]["session_id"],
            "preprocessing_mapping": config_data["data_preprocessing_model"]["preprocessing_mapping"],
            "aws_role": config_data["data_preprocessing_model"]["aws_role"],
            "train_test_split_ratio": config_data["data_preprocessing_model"]["train_test_split_ratio"],
            "instance_type": config_data["data_preprocessing_model"]["instance_type"],
            "instance_count": config_data["data_preprocessing_model"]["instance_count"],
            "secretsManagerArn": config_data["data_preprocessing_model"]["secretsManagerArn"],
            "s3_output_location": config_data["data_ingestion_model"]["s3_ouptut_location"],
        }

        file_upload_result = obj.upload_main_file(data_ingestion_input, data_preprocessing_input)
        if file_upload_result["status"] == "success":
            pipeline_run_status = obj.run_pipeline(data_ingestion_input, data_preprocessing_input)
            print(pipeline_run_status)
            
            return pipeline_run_status
        return file_upload_result

    except Exception as e:
        print(e)
