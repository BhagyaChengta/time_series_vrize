from typing import Any
from fastapi import APIRouter
from app.models.session_model.model import SessionTokenResponse
import os
import json

router = APIRouter()


@router.get("/create", status_code=200, response_model=SessionTokenResponse)
def get_session_token() -> Any:
    session = SessionTokenResponse()
    os.makedirs(os.path.join(os.getcwd(),'app', 'data',session.session_id), exist_ok=True)
    json.dump(session.dict(), open(os.path.join(os.getcwd(),'app', 'data',session.session_id, 'config.json'), 'w'))
    return session

