from enum import Enum

from pydantic import BaseModel


class Status(str, Enum):
    Running = "Running"
    Finished = "Finished"
    Failed = "Failed"


class LogLevel(str, Enum):
    Info = "Info"
    Warning = "Warning"
    Error = "Error"


class EirOut(BaseModel):
    msg: str


class ExpIdOut(BaseModel):
    msg: str


class BusinessLog(BaseModel):
    Level: LogLevel
    Message: str
    Exception: str

    class Config:
        use_enum_values = True
        arbitrary_types_allowed = True


class StatusOut(BaseModel):
    Status: Status
    BusinessLogs: list[BusinessLog]

    class Config:
        use_enum_values = True
        arbitrary_types_allowed = True
