
from pydantic import BaseModel, HttpUrl , Field
from typing import Union, Optional, Any
from enum import Enum


class AvailablePreprocessingMethods_Response(BaseModel):
    result : list[str]


class General_Response(BaseModel):
    status : str
    error : Optional[Any]

class IngestedDataDetails(BaseModel):
    session_id: str = ...
    
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
    connection_params: Union[SnowflakeCreds, MySQLCreds]


class InputTableDetails(BaseModel):
    db_schema: str = ...
    table_name: str = ...
    column_names : list[str] = Field(..., max_items=2, min_items=2)


class PreprocessingColumnsMapping(BaseModel):
    column_name : str = ...
    preprocessing_method : str = ...

class ApplyPreprocessing(BaseModel):
    session_id: str = ...
    preprocessing_mapping : list[PreprocessingColumnsMapping] = Field(..., max_items=2, min_items=2)
    aws_role: str = ...
    train_test_split_ratio: str = "0.8"
    instance_type: Optional[str] = "ml.t3.large"
    instance_count: Optional[int] = 1
    secretsManagerArn: Optional[str]
    s3_output_location: str=...
    
