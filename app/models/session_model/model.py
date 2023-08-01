
from pydantic import  BaseModel
import uuid

class SessionTokenResponse(BaseModel):
    session_id : str = uuid.uuid4().hex


