import asyncio
import json
import os
import mysql.connector
from typing import Any, Optional
import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.models.models import AvailableDbConnections_Response,MySQLCreds, General_Response , InputDbDetails , DbChoices, InputTableDetails, DataIngestionInputModel
from app.controller.db.db import SnowflakeConnector,Dbbase, MySQLConnector
from app.controller.data_ingestion.data_ingestion import DataIngestion
from pydantic import ValidationError


router = APIRouter()
RECIPE_SUBREDDITS = ["recipes", "easyrecipes", "TopSecretRecipes"]

AVAILABLE_DB_CONNECTIONS = ["snowflake", "mysql", "posgres"]


@router.get("/available-db-conns", status_code=200, response_model=AvailableDbConnections_Response)
def get_available_dbs_connections() -> Any:
    return {"result": AVAILABLE_DB_CONNECTIONS}

@router.post("/check-conn", status_code=200, response_model=General_Response)
def check_db_connection(dataset_source_type: str, *, db: InputDbDetails) -> Any:
    print(db)

    try:
        conn_status, desc = False, ''

        if dataset_source_type == 'snowflake':
            obj_db = SnowflakeConnector(
                username=db.connection_params.username,
                password=db.connection_params.password,
                account=db.connection_params.account,
                port=db.connection_params.port,
                warehouse=db.connection_params.warehouse,
                database=db.connection_params.database,
                db_schema=db.connection_params.db_schema,
                storage_integration=db.connection_params.storage_integration
            )
            conn_status, desc = obj_db.check_connection()
            
        elif dataset_source_type == 'mysql':
            obj_db = MySQLConnector(
                username=db.connection_params.username,
                password=db.connection_params.password,
                host=db.connection_params.host,
                port=db.connection_params.port,
                database=db.connection_params.database
            )
            conn_status, desc = obj_db.check_connection()
            
            
        else:
            return {"status": "failed", "error": "Invalid dataset source type"}

        if conn_status:
            config_file_path = os.path.join(os.getcwd(), 'app', 'data', db.session_id, 'config.json')
            config = json.loads(open(config_file_path).read())
            config['dataset_source_type'] = db.dataset_source_type
            config['connection_params'] = db.connection_params.dict()
            with open(config_file_path, 'w') as f:
                json.dump(config, f, indent=4, separators=(',', ': '))

            return {"status": "success"}
        else:
            return {"status": "failed", "error": desc}
    except Exception as e:
        return {"status": "failed", "error": str(e)}
    


@router.post("/ingest-data", status_code=200)
def data_ingestion(*,data: DataIngestionInputModel) -> Any:

    try:
 
        obj = DataIngestion(data)
        file_upload_result = obj.upload_main_file()
        if file_upload_result['status']=='success':
            pipeline_run_status = obj.run_pipeline()

            print(pipeline_run_status)
            if pipeline_run_status['status']=='success':
                config_file_path=os.path.join(os.getcwd(),'app','data',data.session_id,'config.json')
                config = json.loads(open(config_file_path).read())
                config['raw_data_file_path'] = data.s3_ouptut_location + f'raw_data/{data.session_id}'
                config['data_ingestion_model'] = data.dict()
                with open(config_file_path, 'w') as f:
                    json.dump(config, f, indent=4, separators=(',', ': '))
            return pipeline_run_status
        
        return file_upload_result


    except Exception as e:
        print(e)

