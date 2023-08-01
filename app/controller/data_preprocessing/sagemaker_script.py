
from dataclasses import dataclass
from enum import Enum
import traceback
import uuid
import logging
import json
import time
import argparse
import sys
import subprocess
from pyspark.sql.types import StructType, StructField, DoubleType, DateType
import boto3
import os

from pyspark.sql.session import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import stddev, mean,row_number,col
from pyspark.ml.feature import StandardScaler, MinMaxScaler
from pyspark.sql.window import Window


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
    
#     # Apply standardization using StandardScaler
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


if __name__=='__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--output_data_s3_path", type=str, required=True)
        parser.add_argument("--input_data_s3_path", type=str, required=True)
        parser.add_argument("--preprocessing_column_mapping", type=str, required=True)
        parser.add_argument("--train_test_split_ratio", type=str, required=True)
        


        args = parser.parse_args()

        preprocess_mapping = json.loads(args.preprocessing_column_mapping)
        output_data_s3_path = args.output_data_s3_path
        input_data_s3_path = args.input_data_s3_path
        train_test_split_ratio = args.train_test_split_ratio
        timestamp_column_name = preprocess_mapping[0]["column_name"]

        print('done with arguments')

        spark = SparkSession.builder.master("local").getOrCreate()
    


        schema = StructType([
            StructField("Date", DateType(), nullable=True),
            StructField("Price", DoubleType(), nullable=True),
            
        ])

        df = spark.read.format("csv").schema(schema).load(input_data_s3_path, header=True)
        print('The dataset:',df)

        df = df.orderBy(timestamp_column_name)
        print(df)
        split_ratio = int(float(train_test_split_ratio) * 100)
        # Use split_ratio as an integer in your code
        split_point = int(df.count() * (split_ratio / 100))

    
        # split_point = int(df.count() * train_test_split_ratio)
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
    

    except Exception as e:
        logging.error(e)
        traceback.print_exc()
        raise e
