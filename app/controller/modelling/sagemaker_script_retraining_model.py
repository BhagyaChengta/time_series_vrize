import argparse
import sys
import subprocess
import os
import csv
import json
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
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField, DoubleType, DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev, mean,row_number,col
from pyspark.ml.feature import StandardScaler, MinMaxScaler
from pyspark.sql.window import Window
import logging
import tarfile
import traceback
import datetime




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

#p & q value functions
def get_p_range(data, d_value):
    for _ in range(d_value):
        data = data.diff().fillna(0)
    pacf_res = pacf(data, alpha = 0.05)
    err = error_range(pacf_res[1])
    pacf_vals = pacf_res[0]
    sig_lags = find_sig_lags(err, pacf_vals)
    return sig_lags

def get_q_range(data, d_value):
    for _ in range(d_value):
        data = data.diff().fillna(0)
    acf_res = acf(data, alpha = 0.05)
    err = error_range(acf_res[1])
    acf_vals = acf_res[0]
    sig_lags = find_sig_lags(err, acf_vals)
    return sig_lags
    
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

    # Upload the .pkl file to S3
    s3_bucket = 'time-series-data-ingested'
    s3_key = 'model/model.pkl'
    # Load the .pkl file from S3
    s3_client = boto3.client('s3')
    loaded_model = None
    with open('downloaded_model.pkl', 'wb') as f:
        s3_client.download_file(s3_bucket, s3_key, 'downloaded_model.pkl')

    with open('downloaded_model.pkl', 'rb') as f:
        model = pickle.load(f)


    for i in range(len(test)):
        model= ARIMA(curr, order = test_order, enforce_stationarity=False, enforce_invertibility=False)
        model_fit = model.fit(method_kwargs={"warn_convergence": False})
        next_pred = model_fit.predict(start = train_size + i)[0]
        predictions.append(next_pred)
        curr.append(test[i])

     # Save the model as a .pkl file

    current_date = datetime.date.today()
    current_date = current_date.strftime('%Y-%m-%d')

    filename = 'model.pkl'
    with open(filename, 'wb') as f:
        pickle.dump(model, f)
    # Upload the .pkl file to S3
    s3_bucket = 'time-series-data-ingested'
    s3_file_path  = 'existing_model/'+ current_date +'/model.pkl'

    s3_client = boto3.client('s3')
    with open(filename, 'rb') as file_obj:
        s3_client.upload_fileobj(file_obj, s3_bucket, s3_file_path)

    error = mape(test, predictions)
    return (error, test_order)





def conn(args):
    subprocess.check_call([sys.executable, "-m", "pip", "install", "mysql-connector-python"])
    import mysql.connector
    
    # Connect to RDS MySQL
    connection = mysql.connector.connect(
        host=args.host,
        username=args.username,
        password=args.password,
        database=args.database
    )
    cursor = connection.cursor()
    # Fetch data from a table
    cursor.execute(args.queryString)
    data = cursor.fetchall()
    print(data)
    # Convert data to Spark DataFrame
    schema=["date","price"]
    df = spark.createDataFrame(data, schema=schema)
    csv_output_path = args.s3OutputLocation + "/"
    df.write.csv(csv_output_path, header=True, mode="overwrite")
    # Close connections
    cursor.close()
    connection.close()

    print("Data fetched from RDS MySQL and saved to S3 successfully.")
    


def normalisation(df,input_column,s3_path):
    min_max_scalers  = []

    # Apply normalization using MinMaxScaler
    scaler = MinMaxScaler(inputCol=input_column, outputCol=input_column)#f"{column}_normalized"
    scaler_model = scaler.fit(df)
    
    # Apply normalization to the data
    df = scaler_model.transform(df)
    
    # Save the MinMaxScaler model to S3
    model_path = f"{s3_path}/preprocessing/min_max_scaler_{input_column}"
    scaler_model.write().overwrite().save(model_path)
    min_max_scalers.append((input_column, model_path))
    
    return df, min_max_scalers

def standardisation(df,input_column,s3_path):
    # Standardization
    standard_scalers = []

    scaler = StandardScaler(inputCol=input_column, outputCol=input_column,
                            withStd=True, withMean=True) #f"{column}_standardized"
    scaler_model = scaler.fit(df)
    
#     # Apply standardization to the data
    df = scaler_model.transform(df)
    sample= df.head()
    print(sample)
    
#     # Save the StandardScaler model to S3
    model_path = f"{s3_path}/preprocessing/standard_scaler_{input_column}"
    scaler_model.write().overwrite().save(model_path)
    standard_scalers.append((input_column, model_path))

    print("standard_scalers: ",standard_scalers)
    print("returning the df and standard_scalers")

    return df, standard_scalers



def pkl_to_tar_gz(pkl_file_s3_path, tar_gz_file_path):
       # Initialize the S3 client
    s3_client = boto3.client('s3')

    # Get the bucket name and key from the S3 file path
    bucket_name, s3_key = pkl_file_s3_path.replace("s3://", "").split("/", 1)
    
    # Download the .pkl file from S3 to local filesystem
    local_pkl_file_path = 'local_model.pkl'
    with open(local_pkl_file_path, 'wb') as f:
        s3_client.download_fileobj(bucket_name, s3_key, f)

    # Create a tar.gz file with the .pkl file inside
    tar_gz_file_name = 'model.tar.gz'
    with tarfile.open(tar_gz_file_name, 'w:gz') as tar:
        tar.add(local_pkl_file_path, arcname=os.path.basename(local_pkl_file_path))

    # Upload the tar.gz file to S3
    s3_key_tar = 'model/model.tar.gz'
    with open(tar_gz_file_name, 'rb') as f:
        s3_client.upload_fileobj(f, bucket_name, s3_key_tar)

    # Remove the downloaded .pkl and tar.gz files from the local filesystem
    os.remove(local_pkl_file_path)
    os.remove(tar_gz_file_name)
    print("overwritten of tar file is completed")






if __name__ == "__main__":
    try:

        parser = argparse.ArgumentParser()
        parser.add_argument("--datasetSourceType", type=str, required=True)
        parser.add_argument("--host", type=str, required=True)
        parser.add_argument("--username", type=str, required=True)
        parser.add_argument("--password", type=str, required=True)
        parser.add_argument("--database", type=str, required=True)
        parser.add_argument("--s3OutputLocation", type=str, required=True)
        parser.add_argument("--queryString", type=str, required=False)
        parser.add_argument("--session_id", type=str, required=False)
        parser.add_argument("--input_data_s3_path", type=str, required=True)
        parser.add_argument("--preprocessing_column_mapping", type=str, required=True)
        parser.add_argument("--train_test_split_ratio", type=str, required=True)
            
    
        args = parser.parse_args()
        preprocess_mapping = json.loads(args.preprocessing_column_mapping)
        output_data_s3_path = args.s3OutputLocation
        input_data_s3_path = args.input_data_s3_path
        train_test_split_ratio = args.train_test_split_ratio
        timestamp_column_name = preprocess_mapping[0]["column_name"]

        print('done with arguments')

        spark = SparkSession.builder.appName("retraining").getOrCreate()

        conn(args)

        schema = StructType([
                StructField("Date", DateType(), nullable=True),
                StructField("Price", DoubleType(), nullable=True),
                
            ])

        df = spark.read.format("csv").schema(schema).load(input_data_s3_path, header=True)

        print('The dataset:',df)
        print(df.count())

        df = df.orderBy(timestamp_column_name)
        print(df)
        split_ratio = int(float(train_test_split_ratio) * 100)
        # Use split_ratio as an integer in your code
        split_point = int(df.count() * (split_ratio / 100))

        window = Window.orderBy(timestamp_column_name)
        train_data = df.withColumn("row_number", row_number().over(window)).where(col("row_number") <= split_point).drop("row_number")
        print(train_data)
        test_data = df.withColumn("row_number", row_number().over(window)).where(col("row_number") > split_point).drop("row_number")
        print(test_data)
        print("train and test data are separated")

        for preprocess_type, input_column in preprocess_mapping:
            if preprocess_type=='normalization':
                train_data, min_max_scalers = normalisation(train_data,input_column,output_data_s3_path)
                logging.info(f"min_max_scalers: {min_max_scalers}")

            elif preprocess_type=='standardization':
                train_data, standard_scalers = standardisation(train_data,input_column,output_data_s3_path)
                logging.info(f"standard_scalers: {standard_scalers}")

        print("writing the data into s3")

        logging.info("Writing processed data to S3")
        train_data.write.csv(f"{output_data_s3_path}/processed_data/train/", header=True, mode="overwrite")
        test_data.write.csv(f"{output_data_s3_path}/processed_data/test/", header=True, mode="overwrite")

            
        s3_prefix = input_data_s3_path.replace("s3://time-series-data-ingested/", "")
        s3_bucket='time-series-data-ingested'
        s3_prefix = s3_prefix +'processed_data/train/'
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
        print("dataset:",df)

        # Check the format of the loaded time series
        if df.shape[1] != 2 or df["time"].dtype != np.dtype('datetime64[ns]'):
            raise Exception("Not a valid time-series format")
        
        d = "string"
        p = "string"
        q = "string"

        s3_client = boto3.client('s3')
        bucket_name = 'time-series-data-ingested'
        json_file_key = 'model_training/'+args.session_id+'/data.json'

        response = s3_client.get_object(Bucket=bucket_name, Key=json_file_key)
        print("pdq are:", response)

        for line in response['Body'].iter_lines():
            if line.strip():  # Skip empty lines
                try:
                    json_data = json.loads(line)
                    # Process the JSON data for each line
                except json.JSONDecodeError:
                    inner_string = line[1:-1]  # Remove the leading and trailing parentheses
                    inner_list = eval(inner_string)  # Evaluate the string as a Python object

                    # Convert the inner list into JSON format
                    json_data = json.dumps(inner_list)

        print(json_data)
        data_list = ast.literal_eval(json_data)

        # Separate the values
        value1 = data_list[0]
        value2 = data_list[1]

        # Print the variables
        value1, value2, value3 = value2

        # Print the variables
        p=str(value1)
        d=str(value2)
        q=str(value3)
        print("pdq=", p,d,q)

        if d == "string" or p == "string" or q == "string":
            d = None
            p_values = None
            q_values = None
        else:
            p=p+","+p
            q=q+","+q
            d1= (d).replace(" ", "")
            p1= (p).replace(" ", "")
            q1= (q).replace(" ", "")
            d = ast.literal_eval(d1) 
            p_values = ast.literal_eval(p1)
            q_values = ast.literal_eval(q1)


        if d == None or p_values == None or q_values == None:
            d = get_d_val(df.iloc[:, 0])
            p_values, q_values = get_p_range(df.iloc[:, 0], d), get_q_range(df.iloc[:, 0], d)
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
            file_path = 'model_retraining/'+args.session_id +'/data.json'
            # Create an S3 client
            s3_client = boto3.client('s3')
            # Upload the data to S3
            s3_client.put_object(Body=data, Bucket=bucket_name, Key=file_path)


            pkl_file_path = "s3://time-series-data-ingested/model/model.pkl"

            # Replace with the desired output path for .tar.gz file
            tar_gz_file_path = "s3://time-series-data-ingested/model/model.tar.gz"

            # Convert .pkl to .tar.gz
            pkl_to_tar_gz(pkl_file_path, tar_gz_file_path)      

        spark.stop()


    except Exception as e:
        logging.error(e)
        traceback.print_exc()
        raise e
        