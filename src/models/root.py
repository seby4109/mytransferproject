from pydantic import BaseModel


class RootOut(BaseModel):
    status: str
