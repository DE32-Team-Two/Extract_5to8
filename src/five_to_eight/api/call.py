import os
import requests
import pandas as pd

def ice_breaking():
    img = """
    ice!!!!!!
    """
    return img

def get_key():
    key = os.getenv('MOVIE_API_KEY')
    return key
def gen_url(load_dt='20120101', url_param = {}):
    key = get_key()
    base_url = "http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json?key="
    url = base_url + f"{key}&targetDt={load_dt}"
    for k, v in url_param.items():
        url += f"&{k}={v}"

    return url

def req(load_dt='20120101', url_param = {}):
    url = gen_url(load_dt, url_param = {})
    r = requests.get(url)
    code = r.status_code
    data = r.json()

    return code, data

def req2list(load_dt='20120101', url_param = {}) -> list:
    _, data = req(load_dt, url_param) # 생략할 때 사용
    l = data.get('boxOfficeResult').get('dailyBoxOfficeList')
    return l

def list2df(load_dt='20120101', url_param = {}):
    l = req2list(load_dt, url_param)
    df = pd.DataFrame(l)
    return df

def save2df(load_dt='20120101', url_param = {}):

    """airflow 호출 지점"""
    df = list2df(url_param=url_param, load_dt=load_dt)
    df['load_dt'] = load_dt

    df.to_parquet('~/t2/test_parquet', partition_cols=['load_dt'])
    return df
