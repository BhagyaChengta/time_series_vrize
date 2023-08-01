from app.controller.status import StatusJob
from app.models.models import IngestedDataDetails
from fastapi import APIRouter


router = APIRouter()


@router.post("/status/{job_name}")
def get_job_status(job_name: str):
    data = IngestedDataDetails(job_name=job_name)
    status = StatusJob.sagemaker_job_status(data)
    return status
