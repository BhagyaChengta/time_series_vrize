
#importing necessary libraries
import subprocess
import sys
subprocess.check_call([sys.executable, "-m", "pip", "install", "statsmodels","pyspark","matplotlib","fsspec","s3fs"])
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.stattools import adfuller
import pandas as pd
import numpy as np
import matplotlib.pylab as plt
from statsmodels.tsa.stattools import pacf, acf
import concurrent.futures
from statsmodels.tsa.arima.model import ARIMA
from pyspark.sql import SparkSession
import argparse
import boto3
import pickle
import ast
from typing import List
import tarfile



#d-value function 
def get_d_val(data):
    result = adfuller(data)
    currentD = 0
    if (result[1] <= 0.05):
        return 0
    else: 
        while (result[1] > 0.05):
            currentD += 1
            passengers = data.diff().fillna(0)
            result = adfuller(passengers)
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




# def get_p_range(data, d_value):

#     for _ in range(d_value):
#         data = data.diff().fillna(0)
#     pacf_res = pacf(data, alpha = 0.05)
#     err = error_range(pacf_res[1])
#     pacf_vals = pacf_res[0]
#     sig_lags = find_sig_lags(err, pacf_vals)
#     return sig_lags




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




# def get_q_range(data, d_value):
#     for _ in range(d_value):
#         data = data.diff().fillna(0)
#     acf_res = acf(data, alpha = 0.05)
#     err = error_range(acf_res[1])
#     acf_vals = acf_res[0]
#     sig_lags = find_sig_lags(err, acf_vals)
#     return sig_lags

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
    
#evaluation functions
def mape(actual, pred): 
    actual, pred = np.array(actual), np.array(pred)
    return np.mean(np.abs((actual - pred) / actual)) * 100

def evaluate_arima_model(data, test_order):
    data = data.tolist()
    train_size = int(len(data) * 0.7)
    train, test = data[0:train_size], data[train_size:]
    curr = [x for x in train]
    predictions = []
    for i in range(len(test)):
        model = ARIMA(curr, order = test_order, enforce_stationarity=False, enforce_invertibility=False)
        model_fit = model.fit(method_kwargs={"warn_convergence": False})
        next_pred = model_fit.predict(start = train_size + i)[0]
        predictions.append(next_pred)
        curr.append(test[i])

    # Save the model as a .pkl file
    filename = 'model.pkl'
    with open(filename, 'wb') as f:
        pickle.dump(model, f)
    # Upload the .pkl file to S3
    s3_bucket = 'time-series-data-ingested'
    s3_file_path  = 'model/model.pkl'

    s3_client = boto3.client('s3')
    with open(filename, 'rb') as file_obj:
        s3_client.upload_fileobj(file_obj, s3_bucket, s3_file_path)


        
    # Initialize Boto3 S3 client
    s3_client = boto3.client('s3')
    file_key = 'model/model.tar.gz' 
    bucket_name = 'time-series-data-ingested'
    
    # Delete the file in the specified bucket and key location
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=file_key)
        print(f"File '{file_key}' deleted successfully from bucket '{bucket_name}'.")
    except Exception as e:
        print(f"Failed to delete file '{file_key}' from bucket '{bucket_name}': {e}")


    
    # Save the model as a .pkl file
    filename = 'model.pkl'
    with open(filename, 'wb') as f:
        pickle.dump(model, f)

    # Create a tar.gz file containing the model.pkl file
    tar_file_path = 'model.tar.gz'
    with tarfile.open(tar_file_path, 'w:gz') as tar:
        tar.add(filename)

    # Upload the tar.gz file to S3 and overwrite the existing object
    s3_bucket = 'time-series-data-ingested'
    s3_file_path = 'model/model.tar.gz'
    s3_client = boto3.client('s3')

    # Specify the ACL to be 'bucket-owner-full-control' to ensure the bucket owner has full control over the object
    extra_args = {'ACL': 'bucket-owner-full-control'}
    s3 = boto3.resource('s3')
    s3.Bucket(s3_bucket).upload_file(tar_file_path, s3_file_path, ExtraArgs=extra_args)




    error = mape(test, predictions)
    return (error, test_order)


#finding optimal pdq values using multiprocessing

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--session_id", type=str, required=True)
    parser.add_argument("--aws_role", type=str, required=True)
    parser.add_argument("--s3_output_location", type=str, required=True)
    parser.add_argument("--instance_type", type=str, required=True)
    parser.add_argument("--d", type=str)
    parser.add_argument("--p", type=str)
    parser.add_argument("--q", type=str)
    args = parser.parse_args()


    input_data_s3_path = args.s3_output_location.replace("s3://time-series-data-ingested/", "")
    
    spark = SparkSession.builder.appName("sagemaker_script_parameter_tuning").getOrCreate()
    
    s3_bucket = 'time-series-data-ingested'
    s3_prefix = input_data_s3_path

    # Initialize the S3 client
    s3_client = boto3.client('s3')

    # List objects in the bucket and filter based on prefix
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

    # Extract the file name that meets your criteria
    file_name = None
    for obj in response['Contents']:
        if obj['Key'].endswith('.csv'):
            file_name = obj['Key']
            break

    # Check if a valid file name was found
    if file_name is None:
        raise Exception("No CSV file found in the specified S3 bucket and prefix")

    # Read the data from S3 into a DataFrame
    df = pd.read_csv(f's3://{s3_bucket}/{file_name}')
    df["time"] = pd.to_datetime(df.iloc[:, 0], infer_datetime_format=True)
    df.drop(df.columns[0], axis=1, inplace=True)
    df.set_index('time')
    df.iloc[:, 0].fillna(0)

    # Check the format of the loaded time series
    if df.shape[1] != 2 or df["time"].dtype != np.dtype('datetime64[ns]'):
        raise Exception("Not a valid time-series format")
    
    if args.d == "0" or args.p == "0" or args.q == "0":
        d = None
        p_values = None
        q_values = None
    else:
        args.p=args.p+","+args.p
        args.q=args.q+","+args.q
        d1= (args.d).replace(" ", "")
        p1= (args.p).replace(" ", "")
        q1= (args.q).replace(" ", "")
        d = ast.literal_eval(d1) 
        p_values = ast.literal_eval(p1)
        q_values = ast.literal_eval(q1)

    if d == None or p_values == None or q_values == None:
        d = get_d_val(df.iloc[:, 0])
        p_values, q_values = get_imm_p_range(df.iloc[:, 0], d), get_imm_q_range(df.iloc[:, 0], d)
    combs = []
    for p in p_values:
        for q in q_values:
            combs.append((p, d, q))
    with concurrent.futures.ProcessPoolExecutor() as executor:
        #results = executor.map(evaluate_arima_model, repeat(df.iloc[:, 0]), combs)
        results = [executor.submit(evaluate_arima_model, df.iloc[:, 0], comb) for comb in combs]
        minimum = (100, (0, 0, 0))
        i = 1
        for f in concurrent.futures.as_completed(results):
            comb = f.result()
            print(f'{i/len(combs) * 100:.2f} percent of combinations evaluated', end = '\r')
            if minimum[0] > comb[0]:
                minimum = comb
            i += 1
        data = str(minimum)
        # Specify the S3 bucket and file path
        bucket_name = 'time-series-data-ingested'
        file_path = 'model_training/'+args.session_id +'/data.json'
        # Create an S3 client
        s3_client = boto3.client('s3')
        # Upload the data to S3
        s3_client.put_object(Body=data, Bucket=bucket_name, Key=file_path)

    spark.stop()

