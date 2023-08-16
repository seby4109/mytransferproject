import asyncio
import concurrent.futures as futures
import multiprocessing
import queue
import traceback

import pandas as pd
from fastapi import HTTPException
from fastapi import status as st
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from src.custom_exceptions import CalculationError
from src.engine import prepare_data
from src.engine.calc_linear import CalcLinear
from src.engine.effective_calc import EffectiveCalc
from src.engine.get_data import (
    get_business_date,
    get_configuration_id,
    get_dataset_task_id,
    get_id_list,
    select_data,
)
from src.models.eir_model import LogLevel, Status
from src.utils import engine

NUMBER_OF_CORES = multiprocessing.cpu_count()
CALCULATION_TASK = "calculation-task"

# Creating process pool for multiprocessing
pool = futures.ProcessPoolExecutor(max_workers=NUMBER_OF_CORES)

# global queue that stores calculation logs
status_queue = queue.Queue()


def calculation_cancelling(loop: asyncio.AbstractEventLoop):
    global pool
    try:
        (running_calc,) = [
            task
            for task in asyncio.all_tasks(loop)
            if task.get_name() == CALCULATION_TASK
        ]
    except ValueError:
        running_calc = None
    finally:
        if running_calc:
            pool.shutdown(cancel_futures=True)
            running_calc.cancel()
            return {"msg": "Calculation cancelled successfully."}
        else:
            return {"msg": "No calculation in progress."}


def calculation_status(loop: asyncio.AbstractEventLoop):
    """
    Called by "/status/{calculation_task_id}" endpoint.
    Returns status and logs stored in status_queue to .NET API
    accordingly to FIFO method
    """
    pending = [task.get_name() for task in asyncio.all_tasks(loop)]
    if status_queue.empty():
        if CALCULATION_TASK not in pending:
            return JSONResponse(
                status_code=404,
                content={"detail": "No calculation in progress."},
            )
        else:
            return {
                "Status": Status.Running,
                "BusinessLogs": [],
            }
    else:
        return status_queue.get()


async def initialize_calcualtion_endpoint(
    loop: asyncio.AbstractEventLoop,
    calculation_task_id: int,
):
    """
    Main function called by "/calculate/{calculation_task_id}" ednpoint.
    Prepares batches.
    Starts Eir calculation process.
    Updates calculation status queue.
    """
    global pool
    pool = futures.ProcessPoolExecutor(max_workers=NUMBER_OF_CORES)

    # Retriving some parameters based on given CalculationTaskId
    # that are used in other queries and in calculation process
    (
        dataset_task_id,
        previous_dataset_task_id,
        configuration_id,
        business_date,
    ) = await asyncio.gather(
        get_dataset_task_id(
            f"WHERE TaskExecutionId = {calculation_task_id}",
            loop,
        ),
        get_dataset_task_id(
            f"""
            WHERE TaskExecutionId = (
                SELECT PreviousEirCalculationTaskExecutionId
                FROM [Eir.Calc].EirCalculation
                WHERE TaskExecutionId = {calculation_task_id})
            """,
            loop,
        ),
        get_configuration_id(
            f"WHERE TaskExecutionId = {calculation_task_id}",
            loop,
        ),
        get_business_date(
            f"WHERE TaskExecutionId = {calculation_task_id}",
            loop,
        ),
    )

    # Retriving two lists of ids from database.
    # One for ids present in current loadset
    # and one for ids present in previous loadset.
    (id_list, prev_id_list) = await asyncio.gather(
        get_id_list(dataset_task_id, loop),
        get_id_list(previous_dataset_task_id, loop),
    )

    # Creating is_end & is_new lists based on occurence of id
    # in previous and current loadset list
    is_end = prev_id_list.difference(id_list)
    is_new = id_list.difference(prev_id_list)

    # Union sets with ids from both loadsets to get one list
    # with every id that need to be processed in current calcualtion
    id_list = list(id_list.union(prev_id_list))

    status_queue.put(
        {
            "Status": Status.Running,
            "BusinessLogs": [
                {
                    "Level": LogLevel.Info,
                    "Message": "progress: Preparing batches.",
                    "Exception": "",
                },
            ],
        }
    )

    # Creating batches from id_list
    batch_size = 2500
    batches = [
        id_list[i : i + batch_size] for i in range(0, len(id_list), batch_size)
    ]

    # Number of created batches, used for calculation progress monitoring
    num_of_batches = len(batches)

    # Empty set that will be used for pending tasks monitoring
    calculation_tasks = set()

    # Creating new calculation task for every batch from list of batches.
    # Tasks are stored in calculation_tasks set.
    for batch in batches:
        calculation_tasks.add(
            loop.run_in_executor(
                pool,
                run,
                calculation,
                calculation_task_id,
                dataset_task_id,
                previous_dataset_task_id,
                configuration_id,
                is_end,
                is_new,
                business_date,
                batch,
            )
        )

    # Counter for progress monitoring
    counter = 0

    # First input to status queue (progress 0/num_of_batches)
    status_queue.put(
        {
            "Status": Status.Running,
            "BusinessLogs": [
                {
                    "Level": LogLevel.Info,
                    "Message": f"progress: {counter} of {num_of_batches} calculated.",
                    "Exception": "",
                },
            ],
        }
    )

    # During first iteration of this loop tasks are awaited and sent to their
    # own processes. If all processes are busy task will be waiting
    # in task queue until one of processes freed.
    # In next iterations of this loop the done set returned by asyncio.wait
    # is being checked for occurence of any task. If there is one,
    # counter of calcuclated batches increase by one, done task is removed
    # from caculation_tasks and proper logs are inputted to status queue. This
    # process ends, when all tasks are done - when len(calcualtion_tasks) = 0,
    # or is terminated when an error occures
    while len(calculation_tasks):
        done, pending = await asyncio.wait(
            calculation_tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in done:
            counter += 1
            task_result = task.result()

            if type(task_result) is dict:
                pool.shutdown(cancel_futures=True)
                status_queue.put(task_result)
                return
            else:
                status_queue.put(
                    {
                        "Status": Status.Running,
                        "BusinessLogs": [
                            {
                                "Level": LogLevel.Info,
                                "Message": f"progress: {counter} of {num_of_batches} calculated.",
                                "Exception": f"",
                            }
                        ]
                        + task_result,
                    }
                )

        calculation_tasks.difference_update(done)

    # Input final "Finished" status to status queue
    status_queue.put(
        {
            "Status": Status.Finished,
            "BusinessLogs": [
                {
                    "Level": LogLevel.Info,
                    "Message": f"progress: Calculation completed successfully!",
                    "Exception": "",
                },
            ],
        }
    )
    pool.shutdown()


async def calculation(
    calculation_task_id,
    dataset_task_id,
    previous_dataset_task_id,
    configuration_id,
    is_end_pack,
    is_new_pack,
    business_date,
    batch: list,
    loop,
):
    # Retrive input tables for given batch
    (
        exp_info_pack,
        commissions_pack,
        payment_schedule_pack,
        eir_effective_pack,
        commission_settlement_pack,
        effective_settlement_pack,
    ) = await select_data(
        calculation_task_id,
        dataset_task_id,
        previous_dataset_task_id,
        configuration_id,
        batch,
        loop,
    )

    # Create output tables
    results_eir_effective = pd.DataFrame(columns=eir_effective_pack.columns)
    result_effective = pd.DataFrame(columns=effective_settlement_pack.columns)
    result_commission = pd.DataFrame(columns=commission_settlement_pack.columns)

    for id in batch:
        # prepare all data for calculations - filters by id,
        # set settlement dates, and split data for eir, and effective calculation
        try:
            (
                exp_info,
                payment_schedule,
                eir_effective,
                commission_settlement,
                effective_settelment,
                commissions_for_effective,
                commissions_for_linear,
                is_new,
                is_end,
            ) = prepare_data.prepare_data_for_calc(
                id,
                business_date,
                exp_info_pack,
                commissions_pack,
                payment_schedule_pack,
                eir_effective_pack,
                commission_settlement_pack,
                effective_settlement_pack,
                is_new_pack,
                is_end_pack,
            )

            if len(payment_schedule) > 0:
                effective_calc = EffectiveCalc(
                    business_date,
                    id,
                    exp_info,
                    commissions_for_effective,
                    payment_schedule,
                    eir_effective,
                    commission_settlement,
                    is_new,
                    effective_settelment,
                    is_end,
                )
                (
                    rows_effective,
                    rows_commission,
                    rows_eir_effective,
                ) = effective_calc.calculate()

                # add rows from calculation to output tables
                results_eir_effective = pd.concat(
                    [results_eir_effective, rows_eir_effective],
                    ignore_index=True,
                )
                result_effective = pd.concat(
                    [result_effective, rows_effective],
                    ignore_index=True,
                )
                result_commission = pd.concat(
                    [result_commission, rows_commission],
                    ignore_index=True,
                )

            # Calcualte linear comissions
            if len(commissions_for_linear) > 0:
                calc_linear = CalcLinear(
                    loadset_date=business_date,
                    commissions=commissions_for_linear,
                    exp_id=id,
                )
                rows_commission = calc_linear.calulate_commission_settlement(
                    commission_settlement
                )
                result_commission = pd.concat(
                    [result_commission, rows_commission],
                    ignore_index=True,
                )
        except CalculationError as e:
            return {
                "Status": Status.Failed,
                "BusinessLogs": [
                    {
                        "Level": LogLevel.Error,
                        "Message": f"Calculation Error [{type(e).__name__}] occured during calcualtion"
                        + f" of exposure with EirExposureMapId = {id}. {e}",
                        "Exception": traceback.format_exc(),
                    },
                ],
            }
        except Exception as e:
            return {
                "Status": Status.Failed,
                "BusinessLogs": [
                    {
                        "Level": LogLevel.Error,
                        "Message": f"Unexpected Error [{type(e).__name__}] occured during calcualtion"
                        + f" of exposure with EirExposureMapId = {id}. {e}",
                        "Exception": traceback.format_exc(),
                    },
                ],
            }

    # Delete BusinessDate from output tables.
    # Add columns (DatasetTaskExecutionId, CalculationTaskExecutionId)
    # to output tables.
    results_eir_effective = results_eir_effective.drop(columns=["BusinessDate"])
    results_eir_effective["EirDatasetTaskExecutionId"] = dataset_task_id
    results_eir_effective["EirCalculationTaskExecutionId"] = calculation_task_id

    result_effective = result_effective.drop(columns=["BusinessDate"])
    result_effective["EirDatasetTaskExecutionId"] = dataset_task_id
    result_effective["EirCalculationTaskExecutionId"] = calculation_task_id

    result_commission = result_commission.drop(columns=["BusinessDate"])
    result_commission["EirDatasetTaskExecutionId"] = dataset_task_id
    result_commission["EirCalculationTaskExecutionId"] = calculation_task_id

    # Insert results to databse
    try:
        insert_table(results_eir_effective, "EirEffectiveAmortization")
        insert_table(result_effective, "EirEffectiveSettlement")
        insert_table(result_commission, "EirCommissionSettlement")
    except SQLAlchemyError as e:
        return {
            "Status": Status.Failed,
            "BusinessLogs": [
                {
                    "Level": LogLevel.Error,
                    "Message": f"SQL Error [{str(e.__dict__['orig'])}] occured. {e}",
                    "Exception": traceback.format_exc(),
                },
            ],
        }

    return []


def insert_table(df: pd.DataFrame, table: str):
    df.to_sql(
        table,
        con=engine,
        schema="[Eir.Calc]",
        if_exists="append",
        index=False,
    )


def run(corofn, *args):
    """
    wrapper for async functions to run them in executor
    """
    loop = asyncio.new_event_loop()
    try:
        coro = corofn(*args, loop)
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        loop.close()
