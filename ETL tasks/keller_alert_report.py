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
from io import StringIO


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


# Dags arguments
default_args = {
    'owner': 'm-keller-11',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 10, 10),
}

# Shedule
schedule_interval = '*/15 * * * *'

# unique metrics for each part of product
metrics_feed_actions = ['users','views','likes','ctr']
metrics_message_actions = ['users','messages_sent']

# Dag creating
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, tags=['m-keller-11'])
def a_keller_hw7_alerts():
    @task()
    def extract_df_feed():
        query_feed_actions = """
            SELECT
                    toStartOfFifteenMinutes(time) as ts, -- преобразовывает дату к ближайщей 15 минутке
                    toDate(ts) as date,
                    formatDateTime(ts, '%R') as hm, -- преобразовывает дату по 15 минутке
                    count(distinct user_id) as users,
                    countIf(action == 'view') as views,
                    countIf(action == 'like') as likes,
                    (countIf(action == 'like') / countIf(action = 'view')) as ctr
            FROM simulator_20220920.feed_actions
            WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm
            ORDER BY ts
            """
        df_feed_actions = ph.read_clickhouse(query=query_feed_actions,connection=connection)
        return df_feed_actions

    @task()
    def extract_df_message():
        query_message_actions = """
            SELECT
                toStartOfFifteenMinutes(time) as ts, -- преобразовывает дату к ближайщей 15 минутке
                toDate(ts) as date,
                formatDateTime(ts, '%R') as hm, -- преобразовывает дату по 15 минутке
                count(distinct user_id) as users,
                count(reciever_id) as messages_sent
            FROM simulator_20220920.message_actions
            WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm
            ORDER BY ts
        """
        df_message_actions = ph.read_clickhouse(query=query_message_actions,connection=connection)
        return df_message_actions
    
    @task()
    def check_metricks(df, metrics_list):
        '''
            INPUT: df- dataframe, 
                    metrics_list - list of metrics to be analyzed
            OUTPUT: None
            DISCRIPTION: This task checks for anomalies in the data and sends an alert to the chat if they are found.
            '''
        def check_anomaly(df, metric, n=5, a=4):
            '''
            INPUT: df- dataframe, 
                   metric- list of metrics to be analyzed,
                   n- number of fifteen minutes (default value is 5).
                   a- coefficient that determines the width of the interval (default value is 4).
            OUTPUT: is_alert: Bool. 1 - an anomaly is detected. 0 - no anomaly detected.
            DISCRIPTION: This function determines the presence of anomalies based on the interquartile range method.
                        Those for each metric, a confidence interval is constructed, 
                        after which the value of the metric is checked to see if it belongs to this confidence interval.
            '''
            df["q25"] = df[metric].shift(1).rolling(n).quantile(.25)
            df["q75"] = df[metric].shift(1).rolling(n).quantile(.75)
            df["iqr"] = df["q75"] - df["q25"]
            df["up"] = df["q75"] + a * df["iqr"]
            df["low"] =df["q25"] - a * df["iqr"]

            df["up"] = df["up"].rolling(n,center=True, min_periods=1).mean()
            df["low"] = df["low"].rolling(n,center=True, min_periods=1).mean()

            if df[metric].iloc[-1] < df["low"].iloc[-1] or df[metric].iloc[-1] > df["up"].iloc[-1]:
                is_alert = 1
            else:
                is_alert = 0
            return is_alert
        
        for metric in metrics_list:
            tmp_df = df[['ts', metric]].copy()
            is_alert = check_anomaly(tmp_df, metric)

            if is_alert == 1:
                
                current_val = tmp_df[metric].iloc[-1]
                last_val_diff = abs(1- (tmp_df[metric].iloc[-1]/tmp_df[metric].iloc[-2]))
                tmp_df[metric].iloc[-2]                     
                msg = f'❗️Attention❗️\n\
Метрика {metric}\n\
current value {current_val:.2f}\n\
deviation {last_val_diff:.2%}\n\
link for additional information on a dashboard: http://superset.lab.karpov.courses/r/2199'

                sns.set(rc={'figure.figsize': (20, 8)})
                plt.tight_layout()
                ax = sns.lineplot(x = tmp_df['ts'], y = tmp_df[metric], label = metric)
                ax = sns.lineplot(x = tmp_df['ts'], y = tmp_df['up'], label = 'upper limit of the confidence interval')
                ax = sns.lineplot(x = tmp_df['ts'], y = tmp_df['low'], label = 'lower limit of the confidence interval')
                ax.set(xlabel = 'time')
                ax.set_title(f'{metric} and its confidence interval')
                ax.set(ylim=(0, None))


                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = 'report.png'
                plt.close()

                bot.sendMessage(chat_id, text=msg)
                bot.sendPhoto(chat_id, photo=plot_object)

    df_feed_actions = extract_df_feed()
    df_message_actions = extract_df_message()
    check_metricks(df_feed_actions, metrics_feed_actions)
    check_metricks(df_message_actions, metrics_message_actions)
    
a_keller_hw7_alerts = a_keller_hw7_alerts()