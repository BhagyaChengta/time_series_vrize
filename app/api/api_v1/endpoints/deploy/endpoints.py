from app.controller.deployment.deploy import DeploymentModel
from app.models.models import deployModelDetails
from fastapi import APIRouter
from app.core.config import settings as aws_settings
from app.models.models import General_Response


router = APIRouter()


import subprocess

@router.post("/deploy-model", status_code=200)
def deploy_endpoint_route(*, data: deployModelDetails):
    obj = DeploymentModel(data)

    # Deploy the model and return the endpoint
    endpoint_res = obj.deploy_model()
    
    
    return endpoint_res
