import asyncio

import json

import os

from typing import Any, Optional
import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.models.preprocessing_model.models import InputDbDetails,AvailablePreprocessingMethods_Response, DbChoices,General_Response,IngestedDataDetails,ApplyPreprocessing
from app.controller.db.db import MySQLConnector
from app.controller.db.db import SnowflakeConnector
from app.controller.data_ingestion.data_ingestion import DataIngestion
from pydantic import ValidationError
from app.controller.data_preprocessing.data_preprocess import DataPreprocess

router = APIRouter()

AVAILABLE_PREPROCESSING_METHODS = ["standardization", "normalization", "standardization+normalization"]
@router.get("/available-methods", status_code=200, response_model=AvailablePreprocessingMethods_Response)
def get_available_dbs_connections() -> Any:
    return {"result": AVAILABLE_PREPROCESSING_METHODS}


from pydantic import BaseModel

@router.post("/ingested-data-info", status_code=200)
def ingested_data_information( *, data : IngestedDataDetails) -> Any :
    print(data)
    config_file_path = os.path.join(os.getcwd(),'app','data',data.session_id,'config.json')

    # Read the content of the config.json file
    with open(config_file_path, "r") as f:
        config_data = json.load(f)
        dataset_source_type = config_data ["dataset_source_type"]
        confiddata =config_data ["connection_params"]
    try:
        conn_status, desc = False, ''

        if dataset_source_type == 'snowflake':
            obj_db = SnowflakeConnector(
                username=confiddata ["username"],
                password=confiddata["password"],
                account=confiddata ["account"],
                port=confiddata["port"],
                warehouse=confiddata["warehouse"],
                database=confiddata ["database"],
                db_schema=confiddata ["db_schema"],
                storage_integration=confiddata ["storage_integration"]
            )
            conn_status, desc = obj_db.check_connection()

        elif dataset_source_type == 'mysql':
            obj_db = MySQLConnector(
                username=confiddata ["username"],
                password=confiddata ["password"],
                host=confiddata ["host"],
                port=confiddata ["port"],
                database=confiddata ["database"]
            )
            conn_status, desc = obj_db.check_connection()

        else:
            return {"status": "failed", "error": "Invalid dataset source type"}
        if conn_status:
            confi_ingestion=config_data["data_ingestion_model"]

            return  confi_ingestion
        else:
            return {"status": "failed", "error": desc}
    except Exception as e:
        return {"status": "failed", "error": str(e)}
    
@router.post("/apply-preprocessing", status_code=200)
def data_ingestion(data: ApplyPreprocessing):

    config_file_path = os.path.join(os.getcwd(),'app','data',data.session_id,'config.json')

 
    # Read the content of the config.json file
    with open(config_file_path, "r") as f:
        config_data = json.load(f)
        confi_preprocessing=config_data["data_ingestion_model"]
        s3_ouptut_location=confi_preprocessing["s3_ouptut_location"]

        
    try:
        obj = DataPreprocess(data)

        file_upload_result = obj.upload_main_file()
        if file_upload_result['status']=='success':
            pipeline_run_status = obj.run_pipeline()
            print(pipeline_run_status)
            if pipeline_run_status['status']=='success':
                config_file_path=os.path.join(os.getcwd(),'app','data',data.session_id,'config.json')
                config = json.loads(open(config_file_path).read())
                config['raw_data_file_path'] = s3_ouptut_location + f'raw_data/{data.session_id}'
                config['data_preprocessing_model'] = data.dict()
                with open(config_file_path, 'w') as f:
                    json.dump(config, f, indent=4, separators=(',', ': '))
            return pipeline_run_status
        return pipeline_run_status

    except Exception as e:
        print(e)

   