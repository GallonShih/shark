FROM apache/airflow:2.3.4-python3.10
USER root
RUN apt-get update && \
    apt-get install -y \
    gdal-bin \
    libgdal-dev \
    g++
WORKDIR /
COPY ./requirements.txt /requirements.txt
USER airflow
RUN pip install --user --upgrade pip
RUN pip install -r requirements.txt
