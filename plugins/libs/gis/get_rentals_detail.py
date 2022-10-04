# -*- coding: utf-8 -*-

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, INTEGER, VARCHAR, DATE, NUMERIC, TEXT
import geopandas as gpd
from shapely.geometry import Point
import datetime
import pytz
import time
import random
import logging

logger = logging.getLogger(__name__)

class RentalsDetailGetting:
    DOMAIN_URL = 'https://rent.591.com.tw/'
    GET_DETAIL_URL = 'https://bff.591.com.tw/v1/house/rent/detail?id='
    HEADERS = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
    }

    def __init__(self, rental_conn):
        """
        :param rental_conn: gis-db connection
        """
        self.rental_conn = rental_conn
        self.df_rentals_detail = pd.DataFrame(columns=[
            'post_id', 'title', 'countyname', 'townname', 'tags', 'price', 'price_unit',
            'kind', 'area', 'address', 'lat', 'lon', 'desc', 'rule', 'content'
        ])
        logger.info(f"""
            =============================================================
            Let's start to get rentals-detail from 591.com.
            =============================================================
        """)
    
    def _read_rentals(self):
        """
        Get all rentals' post id that want to use api to get detail.
        """
        self.df_rentals = pd.read_sql("""
            SELECT f1.post_id, f1.title, f1.region_id
            FROM rental.rentals f1
            WHERE NOT EXISTS
            (
                SELECT f2.post_id
                FROM rental.rentals_detail f2
                WHERE f1.post_id = f2.post_id
            )
            AND f1.end_date IS NULL
        """, con=self.rental_conn)
    
    def _get_token(self, session):
        """
        :param session: a request session
        :return token: a token 591.com return
        """
        res = session.get(self.DOMAIN_URL, headers=self.HEADERS)
        soup = BeautifulSoup(res.text, 'html.parser')
        token = soup.select_one('meta[name="csrf-token"]').get('content')
        return token

    def _use_rental_detail_api(self, session, headers, id):
        """
        Use 591.com api to get response.
        :param session: a request session
        :param headers: a request headers used in session
        :return res: response of request
        """
        url = self.GET_DETAIL_URL + str(id)
        try:
            res = session.get(url, headers=headers, timeout=10)
        except :
            logger.info(f'This id {id} is not available.')
            res = None
        return res
    
    def _parse_response(self, res, id):
        """
        Parse the response from 591.com api and format it to dataframe.
        :param res: response of request
        :param res: post id of rentals
        """
        if (res.status_code != 200):
            logger.info(f'This id {id} is not available.')
            pass
        else:
            try:
                if res.json()['msg'] in ['物件不存在']:
                    logger.info(f'This id {id} is not available.')
                    pass
                else:
                    rental_data = res.json()['data']
                    title = rental_data['title']
                    countyname = rental_data['breadcrumb'][0]['name']
                    townname = rental_data['breadcrumb'][1]['name']
                    tags = '|'.join([i['value'] for i in rental_data['tags']])
                    price = rental_data['favData']['price']
                    price_unit = rental_data['priceUnit']
                    kind = rental_data['favData'].get('kindTxt', 'NA')
                    area = float(rental_data['favData'].get('area', -1))
                    address = rental_data['favData'].get('address', '')
                    lat = float(rental_data['positionRound'].get('lat', 0))
                    lon = float(rental_data['positionRound'].get('lng', 0))
                    desc = rental_data['service'].get('desc', '')
                    rule = rental_data['service'].get('rule', '')
                    content = BeautifulSoup(rental_data['remark'].get(
                        'content', ''), "html.parser").text
                    df_rentals_detail = pd.DataFrame({
                        'post_id': [id], 'title': [title], 'countyname': [countyname], 'townname': [townname],
                        'tags': [tags], 'price': [price], 'price_unit': [price_unit], 'kind': [kind],
                        'area': [area], 'address': [address], 'lat': [lat], 'lon': [lon],
                        'desc': [desc], 'rule': [rule], 'content': [content]
                    })
                    self.df_rentals_detail = pd.concat([self.df_rentals_detail, df_rentals_detail]).reset_index(drop=True)
            except:
                logger.info(f'Can not get detail from {id}')
                pass
    
    def _get_rentals_detail_all(self):
        """
        Get all rentals's detail after using 591.com api
        """
        logger.info('Start getting rentals detail all.')
        logger.info('Start getting all post id.')
        self._read_rentals()
        logger.info(f'Finish getting all post id: {len(self.df_rentals)}.')
        session = requests.Session()
        session.mount('http://', HTTPAdapter(max_retries=5))
        session.mount('https://', HTTPAdapter(max_retries=5))
        token = self._get_token(session=session)
        cookies = session.cookies
        headers = self.HEADERS.copy()
        headers['X-CSRF-TOKEN'] = token
        headers['deviceid'] = cookies.get_dict()['T591_TOKEN']
        headers['device'] = 'pc'
        logger.info('Start using api to get detail.')
        for idx, data in self.df_rentals.iterrows():
            res = self._use_rental_detail_api(session=session, headers=headers, id=data.post_id)
            self._parse_response(res=res, id=data.post_id)
            time.sleep(random.random())
            if (idx+1) % 200 == 0:
                logger.info(f'Finish number: {(idx+1)}')
        logger.info('Finish using api to get detail.')
        logger.info('Finish getting rentals detail all.')

    def _rentails_detail_to_db(self):
        """
        Save rentals_detail to gis-db
        """
        logger.info('Start Saving to gis-db.')
        geom = [Point(data.lon, data.lat) for idx, data in self.df_rentals_detail.iterrows()]
        self.df_rentals_detail = gpd.GeoDataFrame(self.df_rentals_detail, crs='epsg:4326', geometry=geom)
        self.df_rentals_detail = self.df_rentals_detail.drop(columns=['lon', 'lat']).rename(columns={'geometry': 'coordinates'})
        self.df_rentals_detail.set_geometry('coordinates', inplace=True)
        logger.info('Start inserting new today data.')
        _tw = pytz.timezone('Asia/Taipei')
        today_date = datetime.datetime.now(tz=_tw).date()
        self.df_rentals_detail['update_date'] = today_date
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
        self.df_rentals_detail.to_postgis(name='rentals_detail',
                                        con=self.rental_conn,
                                        schema='rental',
                                        if_exists='append',
                                        index=False,
                                        dtype=df_rentals_detail_type,)
        logger.info(f'Finish inserting new today data. Date: {today_date}')
        logger.info('Finish Saving to gis-db.')

    def execute(self):
        self._get_rentals_detail_all()
        self._rentails_detail_to_db()


if __name__ == '__main__':
    BASIC_FORMAT = "%(asctime)s-%(levelname)s-%(message)s"
    chlr = logging.StreamHandler()
    chlr.setFormatter(logging.Formatter(BASIC_FORMAT))
    logger.setLevel('DEBUG')
    logger.addHandler(chlr)
    rental_conn = create_engine("postgresql://airflow:airflow@127.0.0.1:5434/gis")

    # RentalsDetailGetting
    op = RentalsDetailGetting(rental_conn=rental_conn)
    op.execute()
