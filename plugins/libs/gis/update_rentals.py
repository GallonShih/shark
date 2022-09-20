# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
import datetime
import pytz
import logging

logger = logging.getLogger(__name__)

class RentalsUpdating:
    def __init__(self, rental_conn):
        self.rental_conn =rental_conn
        self.today_date = self._get_today()
    
    def _get_today(self):
        _tw = pytz.timezone('Asia/Taipei')
        today_date = datetime.datetime.now(tz=_tw).date()
        return today_date

    def _insert_new(self):
        logger.info(f'Start inserting new rentals into rental.rentals. Date:{self.today_date}')
        rentals_query = f"""
            INSERT INTO rental.rentals(post_id, title, region_id, start_date, update_date)
            SELECT f1.post_id, f1.title, f1.region_id,
                f1.update_date as start_date, f1.update_date
            FROM rental.rentals_daily f1
            WHERE NOT EXISTS
            (
                SELECT f2.post_id
                FROM rental.rentals f2
                WHERE f1.post_id = f2.post_id
            )
        """
        with self.rental_conn.connect() as con:
            con.execute(rentals_query)
        logger.info('Finish inserting new rentals into rental.rentals.')
    
    def _update_end_date(self):
        logger.info('Start updating end date in rental.rentals.')
        rentals_query = f"""
            UPDATE rental.rentals AS f1
            SET end_date = '{self.today_date}', update_date = '{self.today_date}'
            WHERE NOT EXISTS
            (
                SELECT f2.post_id
                FROM rental.rentals_daily f2
                WHERE f1.post_id = f2.post_id
            )
            AND f1.end_date IS NULL
        """
        with self.rental_conn.connect() as con:
            con.execute(rentals_query)
        logger.info('Finish updating end date in rental.rentals.')
        logger.info('Start updating re-open or mistake rentals.')
        rentals_query = f"""
            UPDATE rental.rentals AS f1
            SET end_date = NULL,  update_date = '{self.today_date}'
            WHERE EXISTS
            (
                SELECT f2.post_id
                FROM rental.rentals_daily f2
                WHERE f1.post_id = f2.post_id
            )
            AND f1.end_date IS NOT NULL
        """
        with self.rental_conn.connect() as con:
            con.execute(rentals_query)
        logger.info('Finish updating re-open or mistake rentals.')
    
    def execute(self):
        self._insert_new()
        self._update_end_date()


if __name__ == '__main__':
    BASIC_FORMAT = "%(asctime)s-%(levelname)s-%(message)s"
    chlr = logging.StreamHandler()
    chlr.setFormatter(logging.Formatter(BASIC_FORMAT))
    logger.setLevel('DEBUG')
    logger.addHandler(chlr)
    rental_conn = create_engine("postgresql://airflow:airflow@127.0.0.1:5434/gis")

    # RentalsUpdating
    op = RentalsUpdating(rental_conn=rental_conn)
    op.execute()
