from pydantic import BaseModel, HttpUrl , Field
from typing import Union, Optional, Any
from enum import Enum


class AvailableDbConnections_Response(BaseModel):
    result : list[str]


class General_Response(BaseModel):
    status : str
    error : Optional[Any]


class DbCreds(BaseModel):
    username: str = ...
    password: str = ...
    port: Optional[int]
    host: Optional[str]
    account: Optional[str]
    warehouse: Optional[str]
    database: Optional[str]
    db_schema: Optional[str]
    storage_integration: Optional[str]


class SnowflakeCreds(DbCreds):
    account: str = ...
    warehouse: Optional[str]
    database: Optional[str]
    db_schema: Optional[str]
    storage_integration: Optional[str]


class MySQLCreds(DbCreds):
    host: str = ...


class DbChoices(Enum):
    snowflake = SnowflakeCreds
    mysql = MySQLCreds


class InputDbDetails(BaseModel):
    session_id: str = ...
    dataset_source_type: str = ...
    connection_params: Union[ MySQLCreds,SnowflakeCreds]


class InputTableDetails(BaseModel):
    db_schema: Optional[str] = ...
    database: str=...
    table_name: str = ...
    column_names : list[str] = Field(..., max_items=2, min_items=2)


class DataIngestionInputModel(InputTableDetails):
    
    session_id: str = ...
    dataset_source_type : str = ...
    s3_ouptut_location: Optional[str] = ...
    aws_role: Optional[str] = ...
    query_string: Optional[str] = None
    instance_type: Optional[str] = "ml.t3.medium"
    instance_count: Optional[int] = 1
    secretsManagerArn: Optional[str]

class IngestedDataDetails(BaseModel):
    job_name: str = ...


class deployModelDetails(BaseModel):
    model_path: str =...
    model_name: str=...
    instance_type: str= ...
    instance_count: Optional[int]= 1
    bucket_name: str=...
    endpoint_config_name: str= ...
    aws_role:str =...
    