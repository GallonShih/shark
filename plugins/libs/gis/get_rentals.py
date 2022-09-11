# -*- coding: utf-8 -*-

import pandas as pd
import requests
from aiohttp import ClientSession, ClientTimeout, TCPConnector
import asyncio
import nest_asyncio
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, INTEGER, VARCHAR
import logging

nest_asyncio.apply()
logger = logging.getLogger(__name__)

class RentalsGetting:
    DOMAIN_URL = 'https://rent.591.com.tw/'
    GET_LIST_URL = 'https://rent.591.com.tw/home/search/rsList'
    GET_DETAIL_URL = 'https://bff.591.com.tw/v1/house/rent/detail?id='
    HEADERS = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
    }

    def __init__(self, rental_conn, region_ids: list):
        """
        :param rental_conn: gis-db connection
        :param region_ids: a list of region_id you want to get from 591.com api
        """
        self.rental_conn = rental_conn
        self.region_ids = region_ids
        self.df_rentals = pd.DataFrame(columns=['post_id', 'title', 'region_id'])
        logger.info(f"""
            =============================================================
            Let's start to get rentals' id from 590.com.
            The regions are {self.region_ids}. 
            =============================================================
        """)


    def _get_token(self, session):
        """
        :param session: a request session
        :return token: a token 591.com return
        """
        res = session.get(self.DOMAIN_URL, headers=self.HEADERS)
        soup = BeautifulSoup(res.text, 'html.parser')
        token = soup.select_one('meta[name="csrf-token"]').get('content')
        return token

    def _get_total_pages(self, session, token, region_id: int):
        """
        :param session: a request session
        :param token: a token 591.com return
        :param region_id: integer, the region you want to get from 591.com api
        :return total_page: the number of total pages of specific region
        :return cookies: the cookies that represent you visit 591.com with specific region
        """
        headers = self.HEADERS.copy()
        headers['X-CSRF-TOKEN'] = token
        c = requests.cookies.RequestsCookieJar()
        c.set('urlJumpIp', f'{region_id}',
                domain='.591.com.tw',
                path='/')
        session.cookies.update(c)
        params = f'is_format_data=1&is_new_list=1&type=1&region={region_id}&firstRow=0'
        res = session.get(self.GET_LIST_URL, params=params, headers=headers)
        total_page = int(res.json()['records'].replace(',', ''))//30+1
        cookies = session.cookies
        return total_page, cookies

    async def _get_rentals_by_page(self, session, region_id: int, page: int):
        """
        :param session: a request session
        :param region_id: integer, the region you want to get from 591.com api
        :param page: the number of page that show in 591.com with specific region
        """
        first_row = 30 * (page-1)
        params = f'is_format_data=1&is_new_list=1&type=1&region={region_id}&firstRow={first_row}'
        async with session.get(self.GET_LIST_URL, params=params) as response:
            if (page % 50 == 0):
                logger.info(f"Page: {page}, Status: {response.status}")
            data = await response.json()
            data = data['data']['data']
            for house in data:
                df_house = pd.DataFrame(data={'post_id': [house['post_id']], 'title': [house['title']], 'region_id': [region_id]})
                self.df_rentals = pd.concat([self.df_rentals, df_house]).reset_index(drop=True)

    def _get_rentals_by_region(self, region_id: int = 1):
        """
        :param region_id: integer, the region you want to get from 591.com api
        """
        logger.info(f"Start get rentals by region {region_id}.")
        with requests.session() as session:
            token = self._get_token(session=session)
            total_page, cookies = self._get_total_pages(
                session=session, token=token, region_id=region_id)
        headers = self.HEADERS.copy()
        headers['X-CSRF-TOKEN'] = token

        async def get_rentals_all_pages():
            timeout = ClientTimeout(600)
            connector = TCPConnector(limit=50)
            async with ClientSession(connector=connector, timeout=timeout, headers=headers, cookies=cookies) as session:
                tasks = [asyncio.create_task(self._get_rentals_by_page(
                    session=session, region_id=region_id, page=page)) for page in range(1, total_page+1)]
                await asyncio.gather(*tasks)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(get_rentals_all_pages())
        logger.info(f"Finish get rentals by region {region_id}.")

    def _get_rentals_all(self):
        logger.info("Start getting all rentals.")
        for region_id in self.region_ids:
            self._get_rentals_by_region(region_id=region_id)
        self.df_rentals.drop_duplicates(subset=['post_id'], keep='last', inplace=True)
        logger.info(f"Finish getting all rentals. rows: {len(self.df_rentals)}.")

    def _rentals_to_do(self):
        logger.info('Start saving to db.')
        with self.rental_conn.connect() as con:
            logger.info('Start truncating rental.rentals.')
            con.execute("""
                TRUNCATE TABLE rental.rentals
            """)
            logger.info('Finish truncating rental.rentals.')
        df_rentals_type = {
            'post_id': BigInteger,
            'title': VARCHAR(128),
            'region_id': INTEGER
        }
        logger.info('Start insert into rental.rentals.')
        self.df_rentals.to_sql(name='rentals',
                            schema='rental',
                            if_exists='append',
                            index=False,
                            dtype=df_rentals_type,
                            con=self.rental_conn,
                            method='multi',
                            chunksize=1000)
        logger.info('Finish insert into rental.rentals.')
        logger.info('Finish saving to db.')

    def execute(self):
        self._get_rentals_all()
        self._rentals_to_do()


if __name__ == '__main__':
    BASIC_FORMAT = "%(asctime)s-%(levelname)s-%(message)s"
    chlr = logging.StreamHandler()
    chlr.setFormatter(logging.Formatter(BASIC_FORMAT))
    logger.setLevel('DEBUG')
    logger.addHandler(chlr)
    rental_conn = create_engine("postgresql://airflow:airflow@127.0.0.1:5434/gis")

    # RentalsGetting
    op = RentalsGetting(rental_conn=rental_conn, region_ids=[1, 3])
    op.execute()
