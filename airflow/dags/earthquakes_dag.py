from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

import io
import pandas as pd
import pyarrow as pa
import requests
import pendulum
import logging

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    level=logging.info,
    datefmt='%Y-%m-%d %H:%M:%S')


default_args = {
    'owner': 'Artem Kozlov',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 5, 15, tz="UTC"),
    'tags': 'earthquakes parsing'
}

DWH_CONN_ID = 'dwh'
DWH_HOOK = PostgresHook(postgres_conn_id=DWH_CONN_ID)

URL = 'https://earthquake.usgs.gov/fdsnws/event/1/query'


@dag(default_args=default_args, description = 'Parsing earthquakes', schedule_interval='30 0 * * *', catchup=True, max_active_runs=1)
def earthquakes():

    @task
    def parsing_data(**context) -> pd.DataFrame:
        """
        Функция парсит данные по землетрясениям за предыдущий день по бесплатному API.
        Данные поступают за все времена, начиная с минимально доступной даты (параметры start_date и Catchup=True).
        Если нет данных в определенную дату, то даг попадает в AirflowSkipException.
        @param: На входе имеем контекст с вычислением даты - вчера;
        @return: Датафрейм с данными по землетрясениям за 1 день.
        """

        today = context['data_interval_end']
        yesterday = context['data_interval_start']

        logging.info(today)
        logging.info(yesterday)

        params = {
            "format": "csv",
            "starttime": yesterday,
            "endtime": today
        }

        response = requests.get(URL, params=params, timeout=10)
        try:
            response.raise_for_status()  # Checks for HTTP request errors
            data = io.StringIO(response.text)
            df = pd.read_csv(data)
            logging.info(f"{URL}?format=csv&starttime={yesterday}&endtime={today}.")
            if df.empty:
                raise AirflowSkipException(f"There is no data for {yesterday}, skipping...")

            return df
        except requests.HTTPError as http_err:
            logging.info(f"HTTP error occurred: {http_err}")
            raise AirflowSkipException("Failed to retrieve data. Skipping this date due to HTTP error.")
        # except Exception as e:
        #     logging.info(f"Other error occurred: {e}")


    @task
    def upload_to_temp_table(df: pd.DataFrame) -> None:
        """
        Функция загружает данные по землетрясениям во временную таблицу в БД. Таблица содержит данные за 1 сутки.
        @param: Датафрейм с данными по землетрясениям за 1 сутки.
        @return: None
        """
        if df is not None:
            df.to_sql(
                'earthquakes',
                DWH_HOOK.get_sqlalchemy_engine(),
                schema = 'staging',
                if_exists = 'replace',
                index=False,
                chunksize = 1000
            )

            logging.info(f"Uploaded {len(df)} records to the database")
    
    @task
    def insert_staging() -> None:
        """
        Функция загружает данные по землетрясениям из временной таблицы БД в историческую таблицу, которая хранит данные за все периоды.
        Есть обработку конфликта по ID и преобразование типов данных.
        @param: None
        @return: None
        """
        sql = """
        INSERT INTO staging.earthquakes_history
        SELECT 
            cast("time" as timestamp) as time, 
            latitude, 
            longitude, 
            depth, 
            mag, 
            "magType" as magtype, 
            nst, gap, 
            dmin, 
            rms, 
            net, 
            id, 
            cast(updated as timestamp) as updated, 
            place, 
            type, 
            "horizontalError", 
            "depthError", 
            "magError", 
            "magNst", 
            status, 
            "locationSource", 
            "magSource"
	    FROM staging.earthquakes
        ON CONFLICT (id) DO UPDATE SET
            time = EXCLUDED.time,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            depth = EXCLUDED.depth,
            mag = EXCLUDED.mag,
            magtype = EXCLUDED.magtype,
            nst = EXCLUDED.nst,
            gap = EXCLUDED.gap,
            dmin = EXCLUDED.dmin,
            rms = EXCLUDED.rms,
            net = EXCLUDED.net,
            updated = EXCLUDED.updated,
            place = EXCLUDED.place,
            type = EXCLUDED.type,
            "horizontalError" = EXCLUDED."horizontalError",
            "depthError" = EXCLUDED."depthError",
            "magError" = EXCLUDED."magError",
            "magNst" = EXCLUDED."magNst",
            status = EXCLUDED.status,
            "locationSource" = EXCLUDED."locationSource",
            "magSource" = EXCLUDED."magSource";
        """
        DWH_HOOK.run(sql)
        logging.info(sql)
        logging.info("Data has been archived into earthquakes_history.")
    
    @task
    def insert_core() -> None:
        """
        Функция загружает данные из денормализованной таблички слоя staging данные в core слой (таблицы фактов и измерений)
        @param: None
        @return: None
        """

        sql = """
        INSERT INTO core.dim_eq_locations
        SELECT 
            id,
	        place,
	        latitude,
	        longitude
	    FROM staging.earthquakes_history
        ON CONFLICT (id) DO UPDATE SET
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            place = EXCLUDED.place;

        INSERT INTO core.fact_eq
        SELECT 
            id,
	        cast("time" as timestamp) as time,
	        mag,
	        depth,
            status
	    FROM staging.earthquakes_history
        ON CONFLICT (id) DO UPDATE SET
            "time" = excluded.time,
            mag = excluded.mag,
            depth = EXCLUDED.depth,
            status = EXCLUDED.status;
        """
        DWH_HOOK.run(sql)
        logging.info(sql)
        logging.info("Data has been added into core tables.")


    df = parsing_data() 
    upload_to_temp_table(df) >> insert_staging() >> insert_core()


dag = earthquakes()

