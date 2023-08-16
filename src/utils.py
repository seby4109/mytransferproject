import urllib.parse
from inspect import currentframe, getframeinfo
from pathlib import Path

import pyodbc
import sqlalchemy

from src.config import cfg

sql_scripts_root = Path(__file__).parent.joinpath("sql")

params = urllib.parse.quote_plus(
    f"DRIVER={cfg.get('DRIVER', '')};{cfg.get('CONNECTION_STRING', '')}"
)
engine = sqlalchemy.create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    fast_executemany=True,
)


def connect_to_db() -> pyodbc.Connection:
    connection_string = (
        f"DRIVER={cfg.get('DRIVER', '')};{cfg.get('CONNECTION_STRING', '')}"
    )
    conn = pyodbc.connect(connection_string)

    return conn


def read_txt(path: Path) -> str:
    with open(path) as f:
        txt = f.read()
    return txt


def error_location():
    frameinfo = getframeinfo(currentframe().f_back)
    filename = frameinfo.filename.split("/")[-1]
    linenumber = frameinfo.lineno
    loc_str = f"File: {filename}, line: {linenumber}"
    return loc_str


class Dotdict(dict):
    """allows dot.notation access to dict elements"""

    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

    def __getattr__(self, attribute_name):
        return self[attribute_name]

    def __repr__(self) -> str:
        return f'<Dotdict(keys=[{", ".join(self.keys())})]>'
