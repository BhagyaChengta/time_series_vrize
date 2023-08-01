

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
import ast




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

    s3_output_location=args.s3_output_location
    input_data_s3_path = s3_output_location.replace("s3://time-series-data-ingested/", "")
    
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
    df.set_index(['time'])
    df.iloc[:,0].fillna(0)
    if df.shape[1] != 2 or df["time"].dtype != np.dtype('datetime64[ns]'):
        raise Exception("Not valid time-series format")
   
    
    
    spark = SparkSession.builder.appName("sagemaker_script_parameter_tuning").getOrCreate()
    if args.d is "0" and args.p is "0" and args.q is "0":
        d = None
        p_values = None
        q_values = None
    else:
        # Split the string values of p and q into lists
        p_list = (args.p).split(",")
        q_list = (args.q).split(",")

        # Convert the elements in p_list to integers
        p_list = [int(value) for value in p_list]
        q_list= [int(value) for value in q_list]
        # Add the new values to the respective lists
        p_list.append(p_list[0])
        q_list.append(q_list[0])

        # Convert the lists back to tuples
        p_values = tuple(p_list)
        q_values = tuple(q_list)

        # Convert d to an integer
        d = int(args.d)

        print(d, p_values[:-1], q_values[:-1])

    if d == None or p_values == None or q_values == None:
        d = get_d_val(df.iloc[:, 0])
        p_values, q_values = get_imm_p_range(df.iloc[:, 0], d), get_imm_q_range(df.iloc[:, 0], d)
    combs = []
    for p in p_values:
        for q in q_values:
            combs.append((p, d, q))
    with concurrent.futures.ProcessPoolExecutor() as executor:
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
        file_path = 'parameter_tuning/'+args.session_id +'/data.json'
        # Create an S3 client
        s3_client = boto3.client('s3')
        # Upload the data to S3
        s3_client.put_object(Body=data, Bucket=bucket_name, Key=file_path)

    spark.stop()
