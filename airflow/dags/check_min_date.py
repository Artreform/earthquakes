import requests
from datetime import datetime

URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"


def find_earliest_year(URL: str) -> int:
    """Функция определяет минимальный год, в котором есть данные по землятресениям
    @param: на вход принимаем адрес сайта с данными по землетрясениям
    @return: возвращаем год в формате int"""

    min_year = 1600
    current_year = 2024

    for year in range(min_year, current_year):
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 12, 31)

        params = {
            "format": "geojson",
            "starttime": start_date.strftime("%Y-%m-%d"),
            "endtime": end_date.strftime("%Y-%m-%d")
        }
        response = requests.get(URL, params=params)

        if response.status_code == 200 and response.json()['features']:
            return year


def find_earliest_date(URL: str, year: int) -> str:
    """Ищет самую раннюю дату землетрясения в указанном году.
    @param: на вход принимаем URL сайта с землетрясением и миниальный год, по которому есть стата
    @return: на выходе получаем минимальную дату"""

    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)

    params = {
        'format': 'geojson',
        'starttime': start_date.strftime('%Y-%m-%d'),
        'endtime': end_date.strftime('%Y-%m-%d'),
        'orderby': 'time-asc',
        'limit': 1
    }

    response = requests.get(URL, params=params)
    if response.status_code == 200 and response.json()['features']:
        earliest_date = datetime.fromtimestamp(response.json()['features'][0]['properties']['time'] / 1000)
        return earliest_date.strftime('%Y-%m-%d')


earliest_year = find_earliest_year(URL)
if earliest_year:
    earliest_date = find_earliest_date(URL, earliest_year)
    print(f"The earliest earthquake data is on: {earliest_date}")