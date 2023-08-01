import argparse
import sys
import subprocess
import os
import csv
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

def main(args):
    subprocess.check_call([sys.executable, "-m", "pip", "install", "mysql-connector-python"])
    import mysql.connector

    spark = SparkSession.builder.appName("MySQL to S3").getOrCreate()
   

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

    # Convert data to Spark DataFrame
    schema=["Date","Price"]
    df = spark.createDataFrame(data, schema=schema)

   
    csv_output_path = args.s3OutputLocation + "/"
    df.write.csv(csv_output_path, header=True)

    # Close connections
    cursor.close()
    connection.close()

    # Write processing job details to a JSON file
   

    print("Data fetched from RDS MySQL and saved to S3 successfully.")
    spark.stop()



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--datasetSourceType", type=str, required=True)
    parser.add_argument("--host", type=str, required=True)
    parser.add_argument("--username", type=str, required=True)
    parser.add_argument("--password", type=str, required=True)
    parser.add_argument("--database", type=str, required=True)
    parser.add_argument("--s3OutputLocation", type=str, required=True)
    parser.add_argument("--queryString", type=str, required=False)
    parser.add_argument("--session_id", type=str, required=False)
  
    args = parser.parse_args()

    main(args)
    