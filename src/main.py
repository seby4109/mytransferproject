import asyncio
from typing import List

from fastapi import HTTPException
from fastapi import status as st

from src import endpoints, models, queries
from src.app import app
from src.endpoints.calculation_endpoints import CALCULATION_TASK

loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
db = queries.Database()


@app.on_event("startup")
async def startup_event():
    await db.connect(loop=loop)


@app.get("/", response_model=models.root.RootOut)
def root():
    return {"status": "ok"}


@app.post(
    "/calculate/{calculation_task_id}",
    response_model=models.eir_model.EirOut,
    status_code=st.HTTP_202_ACCEPTED,
)
async def initialize_calculation(calculation_task_id: int):
    pending = [task.get_name() for task in asyncio.all_tasks(loop)]
    if CALCULATION_TASK in pending:
        raise HTTPException(
            status_code=st.HTTP_425_TOO_EARLY,
            detail="Other calculation task is not finished yet",
        )
    else:
        loop.create_task(
            endpoints.initialize_calcualtion_endpoint(
                loop,
                calculation_task_id,
            ),
            name=CALCULATION_TASK,
        )
        return {
            "msg": f"Calculation with Execution ID: {calculation_task_id} started"
        }


@app.post(
    "/status/{calculation_task_id}",
    response_model=models.eir_model.StatusOut,
    status_code=st.HTTP_200_OK,
)
def status(calculation_task_id):
    return endpoints.calculation_status(loop)


@app.post(
    "/cancel/{calculation_task_id}",
    response_model=models.eir_model.EirOut,
    status_code=st.HTTP_200_OK,
)
def cancelling(calculation_task_id):
    return endpoints.calculation_cancelling(loop)
