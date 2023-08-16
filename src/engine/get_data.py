import asyncio

import pandas as pd

from src import queries


async def select_data(
    calculation_task_id,
    dataset_task_id,
    previous_dataset_task_id,
    configuration_id,
    batch: list[int],
    loop: asyncio.AbstractEventLoop,
):
    """
    Retrieves all data needed for given batch calculations
    """

    # batch id list as string for sql queries
    batch_id_list = "(" + ", ".join(str(id) for id in batch) + ")"

    # where statement for queries
    where_stmt = f"EirExposureMapId IN {batch_id_list}"
    where_stmt_comm = f"Comm.EirExposureMapId IN {batch_id_list}"

    return await asyncio.gather(
        get_exp_info(
            previous_dataset_task_id, dataset_task_id, where_stmt, loop
        ),
        get_commissions(
            calculation_task_id, configuration_id, where_stmt_comm, loop
        ),
        get_payment_schedule(
            previous_dataset_task_id, dataset_task_id, where_stmt, loop
        ),
        get_eir_effective(
            previous_dataset_task_id,
            calculation_task_id,
            where_stmt,
            loop,
        ),
        get_commision_settlement(
            previous_dataset_task_id,
            calculation_task_id,
            where_stmt,
            loop,
        ),
        get_effective_settlement(
            previous_dataset_task_id,
            calculation_task_id,
            where_stmt,
            loop,
        ),
    )


async def get_id_list(dataset_task_id: int, loop: asyncio.AbstractEventLoop):
    query = queries.Query = queries.eir.get_exp_list
    output: pd.DataFrame = await query.execute(
        param_values={"TaskExecutionId": dataset_task_id},
        conditions={},
        loop=loop,
    )
    id_list = set(output["EirExposureMapId"].unique())
    return id_list


async def get_exp_info(
    previous_dataset_task_id,
    dataset_task_id,
    where_stmt: str,
    loop: asyncio.AbstractEventLoop,
):
    query = queries.Query = queries.eir.get_exp_info
    output: pd.DataFrame = await query.execute(
        param_values={
            "prev_data_id": previous_dataset_task_id,
            "data_id": dataset_task_id,
        },
        conditions={"where": where_stmt},
        loop=loop,
    )
    return output


async def get_commissions(
    calculation_task_id: int,
    configuration_id: int,
    where_stmt: str,
    loop: asyncio.AbstractEventLoop,
):
    query = queries.Query = queries.eir.get_commissions
    output: pd.DataFrame = await query.execute(
        param_values={
            "TaskExecutionId": calculation_task_id,
            "ConfigurationId": configuration_id,
        },
        conditions={"where": where_stmt},
        loop=loop,
    )
    return output


async def get_payment_schedule(
    previous_dataset_task_id,
    dataset_task_id,
    where_stmt: str,
    loop: asyncio.AbstractEventLoop,
):
    query = queries.Query = queries.eir.get_payment_schedule
    output: pd.DataFrame = await query.execute(
        param_values={
            "prev_data_id": previous_dataset_task_id,
            "data_id": dataset_task_id,
        },
        conditions={"where": where_stmt},
        loop=loop,
    )
    return output


async def get_eir_effective(
    previous_dataset_task_id,
    calculation_task_id,
    where_stmt: str,
    loop: asyncio.AbstractEventLoop,
):
    query = queries.Query = queries.eir.get_eir_effective
    output: pd.DataFrame = await query.execute(
        param_values={
            "prev_data_id": previous_dataset_task_id,
            "calc_id": calculation_task_id,
        },
        conditions={"where": where_stmt},
        loop=loop,
    )
    return output


async def get_commision_settlement(
    previous_dataset_task_id,
    calculation_task_id,
    where_stmt: str,
    loop: asyncio.AbstractEventLoop,
):
    query = queries.Query = queries.eir.get_commission_settlement
    output: pd.DataFrame = await query.execute(
        param_values={
            "prev_data_id": previous_dataset_task_id,
            "calc_id": calculation_task_id,
        },
        conditions={"where": where_stmt},
        loop=loop,
    )
    return output


async def get_effective_settlement(
    previous_dataset_task_id,
    calculation_task_id,
    where_stmt: str,
    loop: asyncio.AbstractEventLoop,
):
    query = queries.Query = queries.eir.get_effective_settlement
    output: pd.DataFrame = await query.execute(
        param_values={
            "prev_data_id": previous_dataset_task_id,
            "calc_id": calculation_task_id,
        },
        conditions={"where": where_stmt},
        loop=loop,
    )
    return output


async def get_dataset_task_id(
    where_stmt: str,
    loop: asyncio.AbstractEventLoop,
):
    query = queries.Query = queries.eir.get_dataset_task_id
    output: pd.DataFrame = await query.execute(
        param_values={},
        conditions={"where": where_stmt},
        loop=loop,
    )
    if not output.empty:
        task_id: int = int(output.iloc[0, 0])
    else:
        task_id = None
    return task_id


async def get_configuration_id(
    where_stmt: str,
    loop: asyncio.AbstractEventLoop,
):
    query = queries.Query = queries.eir.get_configuration_id
    output: pd.DataFrame = await query.execute(
        param_values={},
        conditions={"where": where_stmt},
        loop=loop,
    )
    return int(output.iloc[0, 0])


async def get_business_date(
    where_stmt: str,
    loop: asyncio.AbstractEventLoop,
):
    query = queries.Query = queries.eir.get_business_date
    output: pd.DataFrame = await query.execute(
        param_values={},
        conditions={"where": where_stmt},
        loop=loop,
    )
    return output.iloc[0, 0]
