"""This is fox trade"""
from datetime import datetime, timedelta
from pprint import pprint
from urllib.parse import urlencode

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator


def gen_sec_id(raw_code: str) -> str:
    """
    生成东方财富专用的sec id
    :param raw_code: 6位股票代码
    :return: 指定格式的字符串
    """
    # 沪市指数
    if raw_code[:3] == '000':
        return f'1.{raw_code}'
    # 深证指数
    if raw_code[:3] == '399':
        return f'0.{raw_code}'
    # 沪市股票
    if raw_code[0] != '6':
        return f'0.{raw_code}'
    # 深市股票
    return f'1.{raw_code}'


def get_k_history(code: str, today: str, klt: int = 101, fqt: int = 1) -> list:
    """
    获取k线数据
    :param code: 6位股票代码
    :param today: 日期 例如 20200201
    :param klt: k线间距 默认为 101 即日k
                klt:1 1分钟
                klt:5 5分钟
                klt:101 日
                klt:102 周
    :param fqt: 复权方式
                不复权 : 0
                前复权 : 1
                后复权 : 2
    :return: k线数据
    """
    east_money_k_lines = {
        'f51': '日期',
        'f52': '开盘',
        'f53': '收盘',
        'f54': '最高',
        'f55': '最低',
        'f56': '成交量',
        'f57': '成交额',
        'f58': '振幅',
        'f59': '涨跌幅',
        'f60': '涨跌额',
        'f61': '换手率',

    }
    east_money_headers = {

        'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; Touch; rv:11.0) like Gecko',
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Referer': 'http://quote.eastmoney.com/center/gridlist.html',
    }
    fields = list(east_money_k_lines.keys())
    fields2 = ",".join(fields)
    sec_id = gen_sec_id(code)
    params = (
        ('fields1', 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13'),
        ('fields2', fields2),
        ('beg', today),
        ('end', today),
        ('rtntype', '6'),
        ('secid', sec_id),
        ('klt', f'{klt}'),
        ('fqt', f'{fqt}'),
    )
    params = dict(params)
    base_url = 'https://push2his.eastmoney.com/api/qt/stock/kline/get'
    url = base_url + '?' + urlencode(params)
    json_response: dict = requests.get(url, headers=east_money_headers).json()

    data = json_response.get('data')
    if data is None:
        if sec_id[0] == '0':
            sec_id = f'1.{code}'
        else:
            sec_id = f'0.{code}'
        params['secid'] = sec_id
        url = base_url + '?' + urlencode(params)
        json_response: dict = requests.get(url, headers=east_money_headers).json()
        data = json_response.get('data')
    if data is None:
        print('股票代码:', code, '可能有误')
        return []

    k_lines = data['klines']

    return k_lines


def get_today_stock_metrics(**context):
    stock_code = "002707"
    today = "20230829"
    stock_metrics_list = get_k_history(stock_code, today)[0].split(",")
    context["ti"].xcom_push(key="日期", value=stock_metrics_list[0])
    context["ti"].xcom_push(key="开盘", value=stock_metrics_list[1])
    context["ti"].xcom_push(key="收盘", value=stock_metrics_list[2])
    context["ti"].xcom_push(key="最高", value=stock_metrics_list[3])
    context["ti"].xcom_push(key="最低", value=stock_metrics_list[4])
    context["ti"].xcom_push(key="成交量", value=stock_metrics_list[5])
    context["ti"].xcom_push(key="成交额", value=stock_metrics_list[6])
    context["ti"].xcom_push(key="振幅", value=stock_metrics_list[7])
    context["ti"].xcom_push(key="涨跌幅", value=stock_metrics_list[8])
    context["ti"].xcom_push(key="涨跌额", value=stock_metrics_list[9])
    context["ti"].xcom_push(key="换手率", value=stock_metrics_list[10])


def reporting(**context):
    pprint(context)


with DAG(
        dag_id="fox_trade",
        start_date=datetime(2023, 8, 29),
        schedule="00 16 * * *",
        catchup=False,
        tags=["fox", "trade"],
        default_args={
            "owner": "admin",
            "retries": 2,
            "retry_delay": timedelta(minutes=5)
        }
) as dag:
    t1 = PythonOperator(
        task_id="get_today_stock_metrics",
        python_callable=get_today_stock_metrics
    )

    t2 = PythonOperator(
        task_id="report_result",
        python_callable=reporting
    )

    t1 >> t2
