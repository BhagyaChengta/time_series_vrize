from fastapi import APIRouter

from app.api.api_v1.endpoints.database import endpoints as db_endpoints
from app.api.api_v1.endpoints.session import endpoints as session_endpoints
from app.api.api_v1.endpoints.preprocessing import endpoints as preprocessing_endpoints
from app.api.api_v1.endpoints.modelling import endpoints as modelling_endpoints
from app.api.api_v1.endpoints.status import endpoints as status_endpoints
from app.api.api_v1.endpoints.deploy import endpoints as deploy_endpoints



api_router = APIRouter()
api_router.include_router(session_endpoints.router, prefix="/session", tags=["Session APIs"])
api_router.include_router(db_endpoints.router, prefix="/db", tags=["Database APIs"])
api_router.include_router(preprocessing_endpoints.router, prefix="/preprocess", tags=["Preprocessing APIs"])
api_router.include_router(modelling_endpoints.router, prefix="/model", tags=["Modelling APIs"])
api_router.include_router(status_endpoints.router, prefix="/Status API", tags=["Status API"])
api_router.include_router(deploy_endpoints.router, prefix="/Deployment API", tags=["DEPLOYMENT API"])


