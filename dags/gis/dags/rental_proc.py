from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from libs.gis.get_rentals import RentalsGetting
from libs.gis.get_rentals_detail import RentalsDetailGetting
from core.config import GIS_RENTAL_GET

default_args = {
    'start_date': datetime(2022, 9, 8),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'gis_db_conn_id': 'gis_postgres',
    'owner': GIS_RENTAL_GET.get('dag_owner_name'),
}

dag = DAG(
    GIS_RENTAL_GET.get('dag_id'),
    schedule_interval=GIS_RENTAL_GET.get('schedule_interval'),
    default_args=default_args,
    catchup=False,
    params={
    }
)

class GetRentals(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        gis_db_conn_id,
        *args,
        **kwargs
    ):
        super(GetRentals, self).__init__(*args, **kwargs)
        self.gis_db_conn_id = gis_db_conn_id

    def execute(self, context):
        db_hook = PostgresHook(self.gis_db_conn_id)
        gis_db_conn = db_hook.get_sqlalchemy_engine()

        proc = RentalsGetting(
            rental_conn=gis_db_conn,
            region_ids=[1, 3]
        )
        proc.execute()

class GetRentalsDetail(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        gis_db_conn_id,
        *args,
        **kwargs
    ):
        super(GetRentalsDetail, self).__init__(*args, **kwargs)
        self.gis_db_conn_id = gis_db_conn_id

    def execute(self, context):
        db_hook = PostgresHook(self.gis_db_conn_id)
        gis_db_conn = db_hook.get_sqlalchemy_engine()

        proc = RentalsDetailGetting(
            rental_conn=gis_db_conn
        )
        proc.execute()

with dag:
    get_rentals = GetRentals(
        task_id='get_rentals'
    )
    get_rentals_detail = GetRentalsDetail(
        task_id='get_rentals_detail'
    )
    get_rentals >> get_rentals_detail
