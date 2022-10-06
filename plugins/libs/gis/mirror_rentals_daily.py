# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, INTEGER, VARCHAR, DATE
import logging

logger = logging.getLogger(__name__)

class RentalsDailyMirroring:
    def __init__(self, rental_conn, remote_conn):
        """
        :param rental_conn: gis-db connection
        :param remote_conn: gis-db in remote cloud service connection
        """
        self.rental_conn = rental_conn
        self.remote_conn = remote_conn
    
    def _truncate_table(self):
        """
        Mirror rental.rentals_daily to remote.
        """
        logger.info('Start truncating rental.rentals_daily in remote.')
        with self.remote_conn.connect() as con:
            con.execute("""
                TRUNCATE TABLE rental.rentals_daily
            """)
        logger.info('Finish truncating rental.rentals_daily in remote.')
    
    def _mirror_rentals_daily(self):
        """
        Mirror rental.rentals_daily from local to remote cloud service.
        """
        logger.info('Start mirroring rentals_daily.')
        logger.info('Start getting rentals_daily from local.')
        df_rentals_daily = pd.read_sql("""
            SELECT *
            FROM rental.rentals_daily
        """, con=self.rental_conn)
        logger.info(f'Finish getting rentals_daily from local: {df_rentals_daily.shape}.')
        df_rentals_daily_type = {
            'post_id': BigInteger,
            'title': VARCHAR(128),
            'region_id': INTEGER,
            'update_date': DATE
        }
        logger.info('Start updating rentals_daily to remote cloud service.')
        df_rentals_daily.to_sql(name='rentals_daily',
                        schema='rental',
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=1000,
                        dtype=df_rentals_daily_type,
                        con=self.remote_conn)
        logger.info('Finish updating rentals_daily to remote cloud service.')
        logger.info('Finish mirroring rentals_daily.')
    
    def execute(self):
        self._truncate_table()
        self._mirror_rentals_daily()


if __name__ == '__main__':
    BASIC_FORMAT = "%(asctime)s-%(levelname)s-%(message)s"
    chlr = logging.StreamHandler()
    chlr.setFormatter(logging.Formatter(BASIC_FORMAT))
    logger.setLevel('DEBUG')
    logger.addHandler(chlr)
    rental_conn = create_engine("postgresql://airflow:airflow@127.0.0.1:5434/gis")
    remote_conn = create_engine("postgresql://postgres:postgres@35.78.72.189:5432/gis")

    # RentalsDailyMirroring
    op = RentalsDailyMirroring(rental_conn=rental_conn, remote_conn=remote_conn)
    op.execute()
