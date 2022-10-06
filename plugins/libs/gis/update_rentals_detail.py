# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, INTEGER, VARCHAR, DATE, NUMERIC, TEXT
import datetime
import pytz
import logging

logger = logging.getLogger(__name__)

class RentalsDetailUpdating:
    def __init__(self, rental_conn, remote_conn):
        """
        :param rental_conn: gis-db connection
        :param remote_conn: gis-db in remote cloud service connection
        """
        self.rental_conn = rental_conn
        self.remote_conn = remote_conn
        self.today_date = datetime.datetime.now(tz = pytz.timezone('Asia/Taipei')).date()
        logger.info(f"""
            Start updating {self.today_date} rental.rentals_detail.
        """)
    
    def _get_rentals_detail(self):
        """
        Get data from rentals_detail of specific date.
        :return df_rentals_detail: dataframe, data from rentals_detail of specific date
        """
        logger.info('Start getting rentals_detail from local.')
        df_rentals_detail = pd.read_sql(f"""
            SELECT *
            FROM rental.rentals_detail f1
            WHERE f1.update_date = '{self.today_date}'
        """, con=self.rental_conn)
        logger.info(f'Finish getting rentals_detail from local {df_rentals_detail.shape}.')
        return df_rentals_detail
    
    def _update_to_remote(self, df_rentals_detail):
        """
        Update rentals_detail in remote.
        :param df_rentals_detail: dataframe, data from rentals_detail of specific date
        """
        logger.info('Start updating to remote.')
        logger.info('Start deleting same date data in remote.')
        with self.remote_conn.connect() as con:
            con.execute(f"""
                DELETE FROM rental.rentals_detail
                WHERE update_date = '{self.today_date}'
            """)
        logger.info('Finish deleting same date data in remote.')
        logger.info(f'Start inserting new today data. Date: {self.today_date}')
        df_rentals_detail_type = {
            'post_id': BigInteger,
            'title': VARCHAR(128),
            'countyname': VARCHAR(3),
            'townname': VARCHAR(5),
            'tags': VARCHAR(128),
            'price': INTEGER,
            'price_unit': VARCHAR(10),
            'kind': VARCHAR(10),
            'area': NUMERIC,
            'address': VARCHAR(128),
            'desc': VARCHAR(128),
            'rule': VARCHAR(128),
            'content': TEXT,
            'update_date': DATE
        }
        df_rentals_detail.to_sql(name='rentals_detail',
                                schema='rental',
                                if_exists='append',
                                index=False,
                                method='multi',
                                chunksize=1000,
                                dtype=df_rentals_detail_type,
                                con=self.remote_conn)
        logger.info(f'Finish inserting new today data. Date: {self.today_date}')
        logger.info('Finish updating to remote.')
    
    def execute(self):
        df_rentals_detail = self._get_rentals_detail()
        self._update_to_remote(df_rentals_detail=df_rentals_detail)


if __name__ == '__main__':
    BASIC_FORMAT = "%(asctime)s-%(levelname)s-%(message)s"
    chlr = logging.StreamHandler()
    chlr.setFormatter(logging.Formatter(BASIC_FORMAT))
    logger.setLevel('DEBUG')
    logger.addHandler(chlr)
    rental_conn = create_engine("postgresql://airflow:airflow@127.0.0.1:5434/gis")
    remote_conn = create_engine("postgresql://postgres:postgres@35.78.72.189:5432/gis")

    # RentalsDetailUpdating
    op = RentalsDetailUpdating(rental_conn=rental_conn, remote_conn=remote_conn)
    op.execute()