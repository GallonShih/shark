FROM apache/airflow:2.3.4-python3.10
WORKDIR /
COPY ./requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install -r requirements.txt