import requests
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

LOGIN = <LOGIN>
YEAR = 1994 + hash(f'{LOGIN}') % 23
CHAT_ID = <CHAT_ID>

default_args = {
    'owner': <OWNER>,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 3, 20),
}

try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = ''

def send_ok_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    data_interval_end = str(context['data_interval_end'])
    tz = timedelta(hours = 3)

    start_time = datetime.strptime(data_interval_end[:-6], '%Y-%m-%dT%H:%M:%S')
    start_time_form = (start_time + tz).strftime("%H:%M:%S")
    delta = datetime.now() - start_time
    delta_sec = delta.total_seconds()

    message = f'Success! Dag {dag_id} was completed on {date}\nstarted at {start_time_form}, execution time: {delta_sec} seconds'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

def send_err_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    data_interval_end = str(context['data_interval_end'])
    tz = timedelta(hours = 3)

    start_time = datetime.strptime(data_interval_end[:-6], '%Y-%m-%dT%H:%M:%S')
    start_time_form = (start_time + tz).strftime("%H:%M:%S")

    message = f'Error! Dag {dag_id} was not completed on {date}\nstarted at {start_time_form}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass


@dag(default_args=default_args, schedule_interval='0 9 * * *', catchup=False, tags=["lesson_3", "flow_15.10.2021"])
def airflow_dag():
    @task(retries=3)
    def get_data():
        df = pd.read_csv('vgsales.csv')
        data = df.query('Year == @YEAR').to_csv(index=False)
        return data

    @task()
    # 1.Какая игра была самой продаваемой в этом году во всем мире?
    def get_max_sold(data):
        df = pd.read_csv(StringIO(data))
        df_r = df.sort_values('Global_Sales', ascending=False)
        #print(df_r['Name'].iloc[0])
        return df_r['Name'].iloc[0]

    @task()
    # 2.Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    def get_top_eu_genre(data):
        df = pd.read_csv(StringIO(data))
        df_r = df.groupby('Genre', as_index=False)\
                 .agg({'EU_Sales': 'sum'})\
                 .sort_values('EU_Sales', ascending=False)
        df_r['EU_Sales'] = df_r['EU_Sales'].round(2)

        max_eu_sales = df_r['EU_Sales'].iloc[0]
        list_top_genres = df_r.query('EU_Sales == @max_eu_sales')['Genre'].tolist()
        return list_top_genres

    @task()
    # 3.На какой платформе было больше всего игр, которые продались более чем
    # миллионным тиражом в Северной Америке? Перечислить все, если их несколько
    def get_top_na_platform(data):
        df = pd.read_csv(StringIO(data))
        df_r = df[df['NA_Sales'] > 1.00]
        df_r = df_r.groupby('Platform', as_index=False)\
                   .agg({'NA_Sales': 'count'})\
                   .sort_values('NA_Sales', ascending=False)
        max_na_platforms = df_r['NA_Sales'].iloc[0]
        list_top_platforms = df_r.query('NA_Sales == @max_na_platforms')['Platform'].tolist()
        return list_top_platforms

    @task()
    # 4.У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    def get_top_publisher_in_jp(data):
        df = pd.read_csv(StringIO(data))
        df_r = df.groupby('Publisher', as_index=False)\
                 .agg({'JP_Sales': 'mean'})\
                 .sort_values('JP_Sales', ascending=False)
        df_r['JP_Sales'] = df_r['JP_Sales'].round(2)

        top_publisher_in_jp = df_r['JP_Sales'].iloc[0]
        list_top_publishers_in_jp = df_r.query('JP_Sales == @top_publisher_in_jp')['Publisher'].tolist()
        return list_top_publishers_in_jp

    @task()
    # 5.Сколько игр продались лучше в Европе, чем в Японии?
    def get_number_eu_gt_jp(data):
        df = pd.read_csv(StringIO(data))
        better_eu_than_jp = df[df['EU_Sales'] > df['JP_Sales']].shape[0]
        return better_eu_than_jp

    @task(on_success_callback=send_ok_message, on_failure_callback=send_err_message)
    def print_data(max_sold, top_eu_genre, top_na_platform, top_publisher_in_jp, number_eu_gt_jp):
        context = get_current_context()
        date = context['ds']
        print(f'Top sold game in the world in {YEAR} year is {max_sold}')
        print(f'Top sold genre in Europe in {YEAR} year is {top_eu_genre}')
        print(f'Top platform in North America by games sold above 1M copies in {YEAR} year is {top_na_platform}')
        print(f'Publisher with max mean number of sold copies in Japan in {YEAR} year is {top_publisher_in_jp}')
        print(f'How many games in {YEAR} year were sold better in Europe than Japan is {number_eu_gt_jp}')

    data = get_data()
    max_sold = get_max_sold(data)
    top_eu_genre = get_top_eu_genre(data)
    top_na_platform = get_top_na_platform(data)
    top_publisher_in_jp = get_top_publisher_in_jp(data)
    number_eu_gt_jp = get_number_eu_gt_jp(data)
    print_data(max_sold, top_eu_genre, top_na_platform, top_publisher_in_jp, number_eu_gt_jp)

airflow_dag = airflow_dag()
