FROM python:3.9.8 AS build

COPY ./requirements.txt requirements.txt

RUN apt-get update && apt-get install -y --no-install-recommends \
    unixodbc-dev \
    unixodbc \
    libpq-dev 

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17

RUN python3 -m pip install -r requirements.txt

COPY ./setup.py setup.py
COPY ./src src
#COPY /.env .env

RUN python3 -m pip install -e .

# run tests
# FROM build as test
# COPY ./requirements_dev.txt requirements_dev.txt
# COPY ./pytest.ini pytest.ini
# COPY ./tests tests
# RUN ["python3", "-m", "pip", "install", "-r", "requirements_dev.txt"]
# RUN ["python3", "-m", "pytest", "-v", "tests"]

# run app
FROM build as eirwebapi
CMD [ "python3", "-m", "uvicorn", "src.main:app", "--port", "80", "--host", "0.0.0.0", "--root-path",  "$PYTHON_ROOT_PATH"]


