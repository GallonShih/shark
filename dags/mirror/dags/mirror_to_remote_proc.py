from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from libs.mirror.mirror_rental_to_remote import RentalMirroring
from core.config import MIRROR_TABLE_TO_REMOTE

default_args = {
    'start_date': datetime(2022, 10, 5),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'gis_db_conn_id': 'gis_postgres',
    'remote_gis_conn_id': 'gis_ec2',
    'owner': MIRROR_TABLE_TO_REMOTE.get('dag_owner_name'),
}

dag = DAG(
    MIRROR_TABLE_TO_REMOTE.get('dag_id'),
    schedule_interval=MIRROR_TABLE_TO_REMOTE.get('schedule_interval'),
    default_args=default_args,
    catchup=False,
    params={
    }
)

class MirrorRental(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        gis_db_conn_id,
        remote_gis_conn_id,
        *args,
        **kwargs
    ):
        super(MirrorRental, self).__init__(*args, **kwargs)
        self.gis_db_conn_id = gis_db_conn_id
        self.remote_gis_conn_id = remote_gis_conn_id

    def execute(self, context):
        db_hook = PostgresHook(self.gis_db_conn_id)
        gis_db_conn = db_hook.get_sqlalchemy_engine()
        db_hook = PostgresHook(self.remote_gis_conn_id)
        remote_gis_conn = db_hook.get_sqlalchemy_engine()

        proc = RentalMirroring(
            rental_conn=gis_db_conn,
            remote_conn=remote_gis_conn
        )
        proc.execute()
    
with dag:
    mirror_rental = MirrorRental(
        task_id='mirror_rental'
    )
    mirror_rental
