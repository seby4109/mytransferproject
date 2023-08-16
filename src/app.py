from typing import Any, Dict

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from src.config import cfg

app = FastAPI(
    docs_url=cfg.get("DOCS_PATH", None),
    root_path=cfg.get("PYTHON_ROOT_PATH", "/"),
    redoc_url=None,
)

# rename post to _post so the original name can be overriden
app._post = app.post
app._get = app.get


def _post_require_response_model(url, *args, response_model=None, **kwargs):
    if not response_model:
        raise TypeError(f'argument response_model is required (path: "{url}")')
    return app._post(url, *args, response_model=response_model, **kwargs)


def _get_require_response_model(url, *args, response_model=None, **kwargs):
    if not response_model:
        raise TypeError(f'argument response_model is required (path: "{url}")')
    return app._get(url, *args, response_model=response_model, **kwargs)


# overwrite
app.post = _post_require_response_model
app.get = _get_require_response_model


def _cast_loc_to_str(dictionary: Dict[Any, Any]) -> Dict[Any, Any]:
    # {'loc': [1,2], 'other':2} -> {'loc': '[1,2]', 'other':2}
    new_dict = {key: value for key, value in dictionary.items() if key != "loc"}
    new_dict["loc"] = str(dictionary.get("loc"))
    return new_dict


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder(
            {
                "detail": [_cast_loc_to_str(item) for item in exc.errors()],
                "body": exc.body,
            }
        ),
    )
