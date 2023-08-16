import asyncio
from pathlib import Path
from string import Formatter
from typing import Any, Dict, List, Optional, Set, Tuple

import aioodbc
import pandas as pd
from async_lru import alru_cache

from src.db import connect_to_db_async, read_sql
from src.utils import Dotdict, read_txt


class Query:
    """
    Supporting class for queries that
    - can verify correctness of parameters (in terms of types)
    - can apply filters to query
    """

    sql_scripts_root = Path(__file__).parent.joinpath("sql")

    def __init__(
        self,
        filepath: str,
        param_order: Optional[Dict[str, Any]] = {},
        param_types: Optional[Dict[str, type]] = {},
    ):
        self.name: str = filepath.replace(".sql", "")
        self.full_path: Path = self.sql_scripts_root.joinpath(Path(filepath))
        self.query_template: str = read_txt(self.full_path)

        self.param_order: Tuple[str] = param_order
        self.param_types: Dict[str, type] = param_types
        self._verify_param_order_length()

    def __repr__(self) -> str:
        return (
            "<Query("
            + ", ".join(
                (
                    f'name="{self.name}"',
                    f'path="{self.full_path}"',
                    f"params={tuple(set(self.param_order))}",
                    f"conditions={tuple(self._read_available_conditions())}",
                )
            )
            + ")>"
        )

    def _verify_param_order_length(self) -> None:
        if len(self.param_order) != self._placeholders_count():
            raise ValueError(
                f'Supplied param_order does not match placeholders in sql query "{self.name}" '
                f"({len(self.param_order)} vs {self._placeholders_count()})."
            )

    def _verify_param_types(self, param_values: Dict[str, Any]) -> None:
        for name in param_values.keys():
            if type(param_values[name]) != self.param_types[name] and type(
                param_values[name]
            ) != type(None):
                raise ValueError(
                    f"Supplied param of wrong type for {name} ({type(param_values[name])} vs {self.param_types[name]})."
                )

    def _read_available_conditions(self) -> Set[str]:
        return {
            item[1]
            for item in Formatter().parse(  # 1-st element is the field name
                self.query_template
            )
        }

    def _verify_conditions(self, supplied_conditions) -> None:
        available = self._read_available_conditions()

        for condition_name in supplied_conditions.keys():
            if condition_name not in available:
                raise ValueError(
                    f"Supplied condition that is not present in query template ({condition_name})"
                )

    def _query_with_conditions(
        self, supplied_conditions: Dict[str, str]
    ) -> str:
        conditions = {
            condition_name: supplied_conditions.get(condition_name, "")
            for condition_name in self._read_available_conditions()
        }
        return self.query_template.format(**conditions)

    def _placeholders_count(self) -> int:
        return self.query_template.count("?")

    def _prepare_param_set(self, param_values: Dict[str, Any]) -> List[Any]:
        """
        maps param_order with param_values dictionary
        example:
        param_order = (name1, name2, name1)
        param_vaues = {name1: jan, name2: janina}
        result: (jan, janina, jan)
        """
        return [param_values.get(key) for key in self.param_order]

    async def execute(
        self,
        param_values: Dict[str, Any],
        conditions: Dict[str, str],
        loop: asyncio.AbstractEventLoop,
    ) -> pd.DataFrame:

        self._verify_param_types(param_values)
        self._verify_conditions(conditions)
        param_set: List[Any] = self._prepare_param_set(param_values)

        query = self._query_with_conditions(conditions)

        result = await self._execute(
            query,
            tuple(param_set),
            loop,
        )
        return result

    @alru_cache
    async def _execute(
        self,
        query: str,
        params: Tuple[Any],
        loop: asyncio.AbstractEventLoop,
    ) -> pd.DataFrame:
        # TODO: consider adding timeout

        connection = await connect_to_db_async(loop)

        result = await read_sql(
            sql=query,
            con=connection,
            params=params,
        )
        await connection.close()
        return result


class Database:
    connection: aioodbc.Connection = None
    loop: asyncio.AbstractEventLoop = None

    def __init__(self) -> None:
        pass

    async def connect(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.connection = await connect_to_db_async(loop)

    async def execute(
        self,
        query,  # TODO: type hint: Query,
        param_values: Optional[Dict[str, Any]] = None,
        conditions: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """additional wrapper to get rid of the eventloop object being passed everywhere"""
        if not param_values:
            param_values = {}

        if not conditions:
            conditions = {}

        result: pd.DataFrame = await query.execute(
            param_values=param_values,
            conditions=conditions,
            loop=self.loop,
        )
        return result


eir = Dotdict(
    {
        "get_exp_list": Query(
            "get_exp_list.sql",
            param_types={"TaskExecutionId": int},
            param_order=("TaskExecutionId",),
        ),
        "get_exp_info": Query(
            "get_exp_info.sql",
            param_types={
                "prev_data_id": int,
                "data_id": int,
            },
            param_order=(
                "prev_data_id",
                "data_id",
            ),
        ),
        "get_commissions": Query(
            "get_commissions.sql",
            param_types={
                "TaskExecutionId": int,
                "ConfigurationId": int,
            },
            param_order=(
                "TaskExecutionId",
                "ConfigurationId",
            ),
        ),
        "get_payment_schedule": Query(
            "get_payment_schedule.sql",
            param_types={
                "prev_data_id": int,
                "data_id": int,
            },
            param_order=(
                "prev_data_id",
                "data_id",
            ),
        ),
        "get_eir_effective": Query(
            "get_eir_effective.sql",
            param_types={
                "prev_data_id": int,
                "calc_id": int,
            },
            param_order=(
                "prev_data_id",
                "calc_id",
            ),
        ),
        "get_commission_settlement": Query(
            "get_commission_settlement.sql",
            param_types={
                "prev_data_id": int,
                "calc_id": int,
            },
            param_order=(
                "prev_data_id",
                "calc_id",
            ),
        ),
        "get_effective_settlement": Query(
            "get_effective_settlement.sql",
            param_types={
                "prev_data_id": int,
                "calc_id": int,
            },
            param_order=(
                "prev_data_id",
                "calc_id",
            ),
        ),
        "get_dataset_task_id": Query(
            "get_dataset_task_id.sql",
            param_types={},
            param_order=(),
        ),
        "get_configuration_id": Query(
            "get_configuration_id.sql",
            param_types={},
            param_order=(),
        ),
        "get_business_date": Query(
            "get_business_date.sql",
            param_types={},
            param_order=(),
        ),
    }
)
