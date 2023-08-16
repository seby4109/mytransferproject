import os
from pathlib import Path

from dotenv import dotenv_values

"""
- default values will be always loaded and can be overriden later
- .env file is used locally, but is not copied into docker containers
- environment variables are used to pass config values to docker 
  containers (this way they can be managed from teamcity/octpus/azure)
"""

default_values = {
    "DRIVER": "{ODBC Driver 17 for SQL Server}",
    "CONNECTION_STRING": "",
    "DEVELOPMENT": 0,
    "PYTHON_ROOT_PATH": "/",
}

cfg_file_path = Path(__file__).parent.joinpath("../.env")

cfg = {
    **default_values,
    **dotenv_values(cfg_file_path),  # load shared development variables
    **os.environ,  # override loaded values with environment variables
}

cfg["DOCS_PATH"] = "/openapi" if cfg.get("DEVELOPMENT") else None
