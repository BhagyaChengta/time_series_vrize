from app.controller.aws.base import AWS
from app.controller.aws.base import AWS

class StatusJob:
    @staticmethod
    def sagemaker_job_status(data):
        sagemaker_session = AWS().get_sage_session()
        status = sagemaker_session.describe_processing_job(data.job_name)['ProcessingJobStatus']
        return {"job_status": status}
