from pydantic import BaseModel, HttpUrl , Field
from typing import Union, Optional, Any,List
from enum import Enum




class AvailableMLModels_Response(BaseModel):
    result : list[str]


class General_Response(BaseModel):

    status : str
    error : Optional[Any]
    info: Optional[str]




class TransformedDataDetails(BaseModel):
    session_id: str = ...

   

class DbCreds(BaseModel):

    username: str = ...
    password: str = ...
    port: Optional[int]
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
    connection_params: DbCreds = ...





class InputTableDetails(BaseModel):

    db_schema: str = ...
    table_name: str = ...
    column_names : list[str] = Field(..., max_items=2, min_items=2)




class PreprocessingColumnsMapping(BaseModel):

    column_name : str = ...
    preprocessing_method : str = ...




class TrainModel(BaseModel):

    session_id: str = ...
    aws_role: str = ...
    # s3_output_location: str=...
    instance_type: Optional[str] = "ml.t3.xlarge"
    instance_count: Optional[int] = 1
    secretsManagerArn: Optional[str]
    d:Optional[list[int]]=[0]
    p:Optional[list[int]]=[0]
    q:Optional[list[int]]=[0]




class HyperParameterTuneModel(BaseModel):

    session_id: str = ...
    aws_role: str = ...
    #s3_output_location: str=...
    instance_type: Optional[str] = "ml.t3.medium"
    instance_count: Optional[int] = 1
    secretsManagerArn: Optional[str]
    d:Optional[list[int]]=[0]
    p_range:Optional[list[int]]=[0]
    q_renge:Optional[list[int]]=[0]

   

class model_arg(BaseModel):
    session_id: str = ...
    aws_role: str = ...
    # s3_output_location: str=...
    secretsManagerArn: Optional[str]

class retraining_model_arg(BaseModel):
    session_id: str = ...
    start_date: str = ...
    end_date : str = ...
