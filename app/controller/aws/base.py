import boto3
import logging
import sagemaker
from app.core.config import settings

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AWS:
    boto3_session = None
    sagemaker_session = None

    def __init__(self) -> None:
        pass

    @classmethod
    
    def create_session(cls):
        cls.boto3_session = boto3.Session(
            aws_access_key_id=settings.AWS_ACCESS_KEY,
            aws_secret_access_key=settings.AWS_SECRET_KEY,
            region_name=settings.AWS_REGION
        )

        cls.sagemaker_session = sagemaker.session.Session(
            boto_session=cls.boto3_session)

    @classmethod
    def get_boto3_session(cls):
        if cls.boto3_session is None:
            logging.info("Creating Boto3 session...")
            cls.create_session()
            logging.info("Boto3 session created.")

        return cls.boto3_session

    @classmethod
    def get_sage_session(cls):
        if cls.sagemaker_session is None:
            logging.info("Creating SageMaker session...")
            cls.create_session()
            logging.info("SageMaker session created.")

        return cls.sagemaker_session
