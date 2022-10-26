from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from libs.gis.get_rentals import RentalsGetting
from libs.gis.update_rentals import RentalsUpdating
from libs.gis.get_rentals_detail import RentalsDetailGetting
from libs.gis.mirror_rentals_daily import RentalsDailyMirroring
from libs.gis.update_rentals_detail import RentalsDetailUpdating
from libs.utils.slack import slack_fail_alert
from core.config import GIS_RENTAL_GET

default_args = {
    'start_date': datetime(2022, 9, 8),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'gis_db_conn_id': 'gis_postgres',
    'remote_gis_conn_id': 'gis_ec2',
    'owner': GIS_RENTAL_GET.get('dag_owner_name'),
    'on_failure_callback': slack_fail_alert,
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

class UpdateRentals(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        gis_db_conn_id,
        *args,
        **kwargs
    ):
        super(UpdateRentals, self).__init__(*args, **kwargs)
        self.gis_db_conn_id = gis_db_conn_id

    def execute(self, context):
        db_hook = PostgresHook(self.gis_db_conn_id)
        gis_db_conn = db_hook.get_sqlalchemy_engine()

        proc = RentalsUpdating(
            rental_conn=gis_db_conn
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

class MirrorRentalsDaily(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        gis_db_conn_id,
        remote_gis_conn_id,
        *args,
        **kwargs
    ):
        super(MirrorRentalsDaily, self).__init__(*args, **kwargs)
        self.gis_db_conn_id = gis_db_conn_id
        self.remote_gis_conn_id = remote_gis_conn_id
    
    def execute(self, context):
        db_hook = PostgresHook(self.gis_db_conn_id)
        gis_db_conn = db_hook.get_sqlalchemy_engine()
        db_hook = PostgresHook(self.remote_gis_conn_id)
        remote_gis_conn = db_hook.get_sqlalchemy_engine()

        proc = RentalsDailyMirroring(
            rental_conn=gis_db_conn,
            remote_conn=remote_gis_conn
        )
        proc.execute()

class UpdateRentalsRemote(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        remote_gis_conn_id,
        *args,
        **kwargs
    ):
        super(UpdateRentalsRemote, self).__init__(*args, **kwargs)
        self.remote_gis_conn_id = remote_gis_conn_id

    def execute(self, context):
        db_hook = PostgresHook(self.remote_gis_conn_id)
        remote_gis_conn = db_hook.get_sqlalchemy_engine()

        proc = RentalsUpdating(
            rental_conn=remote_gis_conn
        )
        proc.execute()

class UpdateRentalsDetail(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        gis_db_conn_id,
        remote_gis_conn_id,
        *args,
        **kwargs
    ):
        super(UpdateRentalsDetail, self).__init__(*args, **kwargs)
        self.gis_db_conn_id = gis_db_conn_id
        self.remote_gis_conn_id = remote_gis_conn_id
    
    def execute(self, context):
        db_hook = PostgresHook(self.gis_db_conn_id)
        gis_db_conn = db_hook.get_sqlalchemy_engine()
        db_hook = PostgresHook(self.remote_gis_conn_id)
        remote_gis_conn = db_hook.get_sqlalchemy_engine()

        proc = RentalsDetailUpdating(
            rental_conn=gis_db_conn,
            remote_conn=remote_gis_conn
        )
        proc.execute()

with dag:
    get_rentals = GetRentals(
        task_id='get_rentals'
    )
    update_rentals = UpdateRentals(
        task_id='update_rentals'
    )
    get_rentals_detail = GetRentalsDetail(
        task_id='get_rentals_detail'
    )
    mirror_rentals_daily = MirrorRentalsDaily(
        task_id='mirror_rentals_daily'
    )
    update_rentals_remote = UpdateRentalsRemote(
        task_id='update_rentals_remote'
    )
    update_rentals_detail = UpdateRentalsDetail(
        task_id='update_rentals_detail_remote'
    )
    get_rentals >> [update_rentals, mirror_rentals_daily]
    update_rentals >> get_rentals_detail
    mirror_rentals_daily >> update_rentals_remote
    [get_rentals_detail, update_rentals_remote] >> update_rentals_detail
