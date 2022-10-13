# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, INTEGER, VARCHAR, DATE, NUMERIC, TEXT
import logging

logger = logging.getLogger(__name__)

class RentalMirroring:
    def __init__(self, rental_conn, remote_conn):
        """
        :param rental_conn: gis-db connection
        :param remote_conn: gis-db in remote cloud service connection
        """
        self.rental_conn = rental_conn
        self.remote_conn = remote_conn

    def _create_table(self):
        """
        Create rental.rentals, rental.rentals_daily, rental.rentals_detail in gis in remote cloud service.
        """
        logger.info('Start creating tables in rental.')
        with self.remote_conn.connect() as con:
            logger.info('Start creating rental.rentals.')
            con.execute("""
                DROP TABLE IF EXISTS rental.rentals;
                CREATE TABLE IF NOT EXISTS rental.rentals
                (
                    post_id bigint NOT NULL,
                    title character varying(128) COLLATE pg_catalog."default",
                    region_id integer NOT NULL,
                    start_date date NOT NULL,
                    end_date date,
                    update_date date NOT NULL,
                    CONSTRAINT rentals_pkey PRIMARY KEY (post_id)
                )

                TABLESPACE pg_default;

                ALTER TABLE IF EXISTS rental.rentals
                    OWNER to postgres;
            """)
            logger.info('Finish creating rental.rentals.')
            logger.info('Start creating rental.rentals_daily.')
            con.execute("""
                DROP TABLE IF EXISTS rental.rentals_daily;
                CREATE TABLE IF NOT EXISTS rental.rentals_daily
                (
                    post_id bigint NOT NULL,
                    title character varying(128) COLLATE pg_catalog."default",
                    region_id integer NOT NULL,
                    update_date date NOT NULL,
                    CONSTRAINT rentals_daily_pkey PRIMARY KEY (post_id)
                )

                TABLESPACE pg_default;

                ALTER TABLE IF EXISTS rental.rentals_daily
                    OWNER to postgres;
            """)
            logger.info('Finish creating rental.rentals_daily.')
            logger.info('Start creating rental.rentals_detail.')
            con.execute("""
                DROP TABLE IF EXISTS rental.rentals_detail;
                CREATE TABLE IF NOT EXISTS rental.rentals_detail
                (
                    post_id bigint NOT NULL,
                    title character varying(128) COLLATE pg_catalog."default",
                    countyname character varying(3) COLLATE pg_catalog."default",
                    townname character varying(5) COLLATE pg_catalog."default",
                    tags character varying(128) COLLATE pg_catalog."default",
                    price integer,
                    price_unit character varying(10) COLLATE pg_catalog."default",
                    kind character varying(10) COLLATE pg_catalog."default",
                    area numeric,
                    address character varying(128) COLLATE pg_catalog."default",
                    "desc" character varying(128) COLLATE pg_catalog."default",
                    rule character varying(128) COLLATE pg_catalog."default",
                    content text COLLATE pg_catalog."default",
                    coordinates geometry(Point,4326),
                    update_date date NOT NULL,
                    CONSTRAINT rentals_detail_pkey PRIMARY KEY (post_id)
                )

                TABLESPACE pg_default;

                ALTER TABLE IF EXISTS rental.rentals_detail
                    OWNER to postgres;
            """)
            logger.info('Finish creating rental.rentals_detail.')
        logger.info('Finish creating tables in rental.')

    def _mirror_rentals(self):
        """
        Mirror rental.rentals from local to remote cloud service.
        """
        logger.info('Start mirroring rentals.')
        logger.info('Start getting rentals from local.')
        df_rentals = pd.read_sql("""
            SELECT *
            FROM rental.rentals
        """, con=self.rental_conn)
        logger.info(f'Finish getting rentals from local: {df_rentals.shape}.')
        df_rentals_type = {
            'post_id': BigInteger,
            'title': VARCHAR(128),
            'region_id': INTEGER,
            'start_date': DATE,
            'end_date': DATE,
            'update_date': DATE
        }
        logger.info('Start updating rentals to remote cloud service.')
        df_rentals.to_sql(name='rentals',
                        schema='rental',
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=1000,
                        dtype=df_rentals_type,
                        con=self.remote_conn)
        logger.info('Finish updating rentals to remote cloud service.')
        logger.info('Finish mirroring rentals.')

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

    def _mirror_rentals_detail(self):
        """
        Mirror rental.rentals_detail from local to remote cloud service.
        """
        logger.info('Start mirroring rentals_detail.')
        logger.info('Start getting rentals_detail from local.')
        df_rentals_detail = pd.read_sql("""
            SELECT *
            FROM rental.rentals_detail
        """, con=self.rental_conn)
        logger.info(f'Finish getting rentals_detail from local: {df_rentals_detail.shape}.')
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
        logger.info('Start updating rentals_detail to remote cloud service.')
        df_rentals_detail.to_sql(name='rentals_detail',
                        schema='rental',
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=1000,
                        dtype=df_rentals_detail_type,
                        con=self.remote_conn)
        logger.info('Finish updating rentals_detail to remote cloud service.')
        logger.info('Finish mirroring rentals_detail.')

    def execute(self):
        self._create_table()
        self._mirror_rentals()
        self._mirror_rentals_daily()
        self._mirror_rentals_detail()


if __name__ == '__main__':
    BASIC_FORMAT = "%(asctime)s-%(levelname)s-%(message)s"
    chlr = logging.StreamHandler()
    chlr.setFormatter(logging.Formatter(BASIC_FORMAT))
    logger.setLevel('DEBUG')
    logger.addHandler(chlr)
    rental_conn = create_engine("postgresql://airflow:airflow@127.0.0.1:5434/gis")
    remote_conn = create_engine("postgresql://***:***@***/gis")

    # RentalMirroring
    op = RentalMirroring(rental_conn=rental_conn, remote_conn=remote_conn)
    op.execute()
