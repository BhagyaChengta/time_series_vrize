import json
from app.controller.aws.base import AWS
from app.models.modelling_models.models import PreprocessingColumnsMapping, TrainModel
from sagemaker.spark.processing import PySparkProcessor
import os
from datetime import date, datetime
import subprocess
import sys
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.stattools import adfuller
import pandas as pd
import numpy as np
import matplotlib.pylab as plt
from statsmodels.tsa.stattools import pacf, acf
import concurrent.futures
from statsmodels.tsa.arima.model import ARIMA
import argparse
import boto3
from app.core.config import settings
from app.controller.aws.helpers import get_s3_path
import io


def json_serial(obj):

    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

class model_arguments:

    def __init__(self, data: TrainModel):
        self.session_id= data.session_id
        self.aws_role= data.aws_role
        self.s3_output_location=get_s3_path(data.session_id,"processed_data/train/")
        self.secretsManagerArn= data.secretsManagerArn
        self.script_path = None
        self.job_name = None
        config_file_path=os.path.join(os.getcwd(),'app','data',data.session_id,'config.json')
        self.config=json.loads(open(config_file_path).read())

     

    def generate_arguments(self):


            input_data_s3_path = self.s3_output_location.replace("s3://time-series-data-ingested/", "")
            
            s3_bucket = 'time-series-data-ingested'
            s3_prefix = input_data_s3_path
            # Initialize the S3 client
            s3 = boto3.client('s3', aws_access_key_id=settings.AWS_ACCESS_KEY, aws_secret_access_key=settings.AWS_SECRET_KEY)
            # List objects in the bucket and filter based on prefix
            response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

            file_name = None
            for obj in response['Contents']:
                if obj['Key'].endswith('.csv'):
                    file_name = obj['Key']
                    break
                # Check if a valid file name was found
            if file_name is None:
                raise Exception("No CSV file found in the specified S3 bucket and prefix")

            # Read the data from S3 into a DataFrame
          
            obj = s3.get_object(Bucket=s3_bucket, Key=file_name)
            df = pd.read_csv(obj['Body'], parse_dates=[0], infer_datetime_format=True)
            df.iloc[:,1]=df.iloc[:,1].astype(int)
            #df = pd.read_csv(io.StringIO(obj))
            df["time"] = pd.to_datetime(df.iloc[:, 0], infer_datetime_format=True)
            df.drop(df.columns[0], axis = 1, inplace = True)
            df.set_index(['time'])
            df.iloc[:,0].fillna(0)
            if df.shape[1] != 2 or df["time"].dtype != np.dtype('datetime64[ns]'):
                raise Exception("Not valid time-series format")
            d = get_d_val(df.iloc[:, 0])
            p_values, q_values = get_imm_p_range(df.iloc[:, 0], d), get_imm_q_range(df.iloc[:, 0], d)
            return(d,p_values,q_values)

       





   

#d-value function

def get_d_val(data):

    result = adfuller(data)
    currentD = 0
    if (result[1] <= 0.05):
        return 0
    else:

        while (result[1] > 0.05):
            currentD += 1
            data = data.diff().fillna(0)
            result = adfuller(data)
            print(result[1])
        bestD = currentD
        return bestD





#helper functions

def find_sig_lags(err_range, function_values):

    arr = []
    for i in range(len(function_values)):
        if function_values[i] >= err_range[i][1] or function_values[i] <= err_range[i][0]:
            arr.append(i)
    return arr




def error_range(conf_int):

    new_arr = []
    for i in range(len(conf_int)):
        moe = (conf_int[i][1]-conf_int[i][0])/2
        new_arr.append([-moe, moe])
    return new_arr




def get_p_range(data, d_value):

    for _ in range(d_value):
        data = data.diff().fillna(0)
    pacf_res = pacf(data, alpha = 0.05)
    err = error_range(pacf_res[1])
    pacf_vals = pacf_res[0]
    sig_lags = find_sig_lags(err, pacf_vals)
    return sig_lags




#Updated to return a smaller range

def get_imm_p_range(data, d_value):

    for _ in range(d_value):
        data = data.diff().fillna(0)
    pacf_res = pacf(data, alpha = 0.05)
    err = error_range(pacf_res[1])
    pacf_vals = pacf_res[0]
    sig_lags = find_sig_lags(err, pacf_vals)
    p_values = []
    i = 1
    for lag in sig_lags[1:]:
        if lag == i:
            p_values.append(lag)
            i += 1
        else:
            break
    return p_values




def get_q_range(data, d_value):
    for _ in range(d_value):
        data = data.diff().fillna(0)
    acf_res = acf(data, alpha = 0.05)
    err = error_range(acf_res[1])
    acf_vals = acf_res[0]
    sig_lags = find_sig_lags(err, acf_vals)
    return sig_lags

#Updated to return a smaller range

def get_imm_q_range(data, d_value):

    for _ in range(d_value):
        data = data.diff().fillna(0)
    acf_res = acf(data, alpha = 0.05)
    err = error_range(acf_res[1])
    acf_vals = acf_res[0]
    sig_lags = find_sig_lags(err, acf_vals)
    q_values = []
    i = 1
    for lag in sig_lags[1:]:
        if lag == i:
            q_values.append(lag)
            i += 1
        else:
            break
    return q_values