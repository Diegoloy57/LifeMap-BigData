FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    "dask[distributed,dataframe]" \
    "bokeh>=3.1.0" \
    pandas \
    numpy \
    pymongo \
    pyarrow \
    kaggle \
    six

COPY . /app