import asyncio
import decimal
import aioodbc
import numpy as np
import pandas as pd

from src.config import cfg


def _get_connection_string() -> str:
    base_connection_string = cfg.get("CONNECTION_STRING")

    if base_connection_string:
        parts = [
            f"DRIVER={cfg.get('DRIVER', '')}",
            base_connection_string,
        ]
        return ";".join(parts)

    raise ValueError("Environment variable 'CONNECTION_STRING' is empty")


async def connect_to_db_async(loop: asyncio.AbstractEventLoop) -> aioodbc.Connection:
    conn = await aioodbc.connect(dsn=_get_connection_string(), loop=loop)
    return conn


async def read_sql(sql: str, con, params) -> pd.DataFrame:
    """
    asynchronous wrapper to replicate pandas.read_sql properties
    """

    cur = await con.cursor()
    await cur.execute(sql, params)

    rows = await cur.fetchall()
    result = pd.DataFrame(
        [list(row) for row in rows], columns=[i[0] for i in cur.description]
    )
    result = _cast_decimal_columns(result)
    await cur.close()
    return result


def _cast_decimal_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    decimal columns from db are not properly handled by numpy, they
    need casting to float types
    """
    df = df.copy()

    columns_to_check = df.select_dtypes(include="object").columns
    for column in columns_to_check:
        if (df[column].apply(type) == decimal.Decimal).any():
            df[column] = df[column].astype(np.float64)
    return df
