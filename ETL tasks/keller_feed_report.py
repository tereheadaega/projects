# import needed libraries
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pandahouse as ph
import requests
import seaborn as sns
import telegram
import io

# access parameters
connection = {
    'host': '*************',
                      'database':'simulator_20220920',
                      'user':'student', 
                      'password':'*********'
                     }

# telegram bot access
bot  = telegram.Bot(token= '*****')

# chat id to which reports will be sent
chat_id = ******

# query for CTR, likes, views and DAU
day_numbers = """
SELECT  CAST(time as DATE) as date,
        count(DISTINCT user_id) as DAU,
        sum(action = 'view') as views,
        sum(action = 'like') as likes,
        likes/views as ctr
FROM simulator_20220920.feed_actions 
WHERE toDate(time) = today() - 1
GROUP BY date
"""
# query for plotting dynamics of metrics changes
week_numbers = """
SELECT  CAST(time as DATE) as date,
        count(DISTINCT user_id) as DAU,
        sum(action = 'view') as views,
        sum(action = 'like') as likes,
        likes/views as ctr
FROM simulator_20220920.feed_actions 
WHERE toDate(time) between today() - 7 and today()-1
GROUP BY date
"""

# Dags arguments
default_args = {
    'owner': 'm-keller-11',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 10, 10),
}

# Shedule
schedule_interval = '0 11 * * *'

# Dag creating
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, tags=['m-keller-11'])
def a_keller_hw7():
    # extract data from the actions table to get the numerical values of the main metrics for yesterday
    @task()
    def extract_day_activity():
        df_day = ph.read_clickhouse(day_numbers, connection=connection)
        return df_day
    # extract data from the table of actions to obtain the calculated indicators of the main indicators for the past week.
    @task
    def extract_week_activity():
        df_week = ph.read_clickhouse(week_numbers, connection=connection)
        return df_week
    # creating text of the report    
    @task
    def message_text(data):
        yesterday = str(data.loc[0,'date'])[:10]
        DAU =data.loc[0,'DAU']
        views = data.loc[0,'views']
        likes = data.loc[0,'likes']
        ctr = round(data.loc[0,'ctr'],4)
        message_test = f'Информация о значениях ключевых метрик за {yesterday}. \n DAU: {DAU} \n views: {views}\
\n likes: {likes} \n CTR: {ctr}'
        return message_test
    # preparing and pushing the report
    @task
    def message_push(message_test,df):
        fig, axes = plt.subplots(2, 2, figsize=(25, 10))
        fig.subplots_adjust(hspace=0.3)
        sns.lineplot(ax=axes[0, 0], data=df, x='date', y='DAU')
        axes[0, 0].set_xlabel('date')
        axes[0, 0].set_ylabel('DAU')
        axes[0, 0].set_title('DAU за последние 7 дней', fontsize=14, fontweight="bold")
        sns.lineplot(ax=axes[0, 1], data=df, x='date', y='views')
        axes[0, 1].set_xlabel('date')
        axes[0, 1].set_ylabel('views')
        axes[0, 1].set_title('Просмотры за последние 7 дней', fontsize=14, fontweight="bold")
        sns.lineplot(ax=axes[1, 0], data=df, x='date', y='ctr')
        axes[1, 0].set_xlabel('date')
        axes[1, 0].set_ylabel('CTR')
        axes[1, 0].set_title('CTR за последние 7 дней', fontsize=14, fontweight="bold")
        sns.lineplot(ax=axes[1, 1], data=df, x='date', y='likes')
        axes[1, 1].set_xlabel('date')
        axes[1, 1].set_ylabel('likes')
        axes[1, 1].set_title('Лайки за последние 7 дней', fontsize=14, fontweight="bold")
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'report.png'
        plt.close()
        bot.sendMessage(chat_id=chat_id, text= message_test)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
    df_day = extract_day_activity()
    df_week = extract_week_activity()
    message_test = message_text(df_day)
    message_push(message_test,df_week)
    
        
a_keller_hw7 = a_keller_hw7()