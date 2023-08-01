from logging import error
import boto3
import logging
import os
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def upload_to_s3(BucketName: str, Content: str, FileName: str, IsFilePath=False, FolderName: str = None, Message: str = None) -> None:
    if IsFilePath:
        _content = open(Content, "rb")

    if type(Content) == 'str':
        _content = Content.encode()
    else:
        _content = Content

    if FolderName:
        FileName = FolderName+"/"+FileName

    s3 = boto3.resource('s3')
    try:
        result = s3.meta.client.put_object(
            Body=_content, Bucket=BucketName, Key=FileName)
        res = result.get('ResponseMetadata')
        
        if res.get('HTTPStatusCode') == 200:
            logging.info('File Uploaded Successfully')
            if Message:
                logging.info(Message)
            return {"status": "success"}
        else:
            logging.error('File Not Uploaded')
            return {"status": "failed", "error": "Problem in Uploading File in S3"}
    except Exception as e:
        logging.exception('Error occurred while uploading file to S3')
        return {"status": "failed", "error": str(e)}



def get_s3_path(session_id, input_key):
    s3_output_location=""

    config_file_path = os.path.join(os.getcwd(), "app", "data", session_id, "config.json")
    with open(config_file_path, "r") as f:
        config_data = json.load(f)
        
    s3_output_location= config_data["data_ingestion_model"]["s3_ouptut_location"]
    resulting_path=s3_output_location+input_key
    print(resulting_path)
    return resulting_path

    