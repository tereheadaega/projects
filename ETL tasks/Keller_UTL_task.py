# import needed libraries
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import pandahouse as ph
import requests


connection = {
    'host': '**************',
                      'database':'simulator_20220920',
                      'user':'student', 
                      'password':'******'
                     }

connection_to_test = {
    'host': '**************',
                      'database':'test',
                      'user':'student-rw', 
                      'password':'*******'
                     }
# Dags arguments
default_args = {
    'owner': 'm-keller-11',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 10, 10),
}
# DAGs schedule 
schedule_interval = '0 8 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, tags=['m-keller-11'])
def aa_keller_hw6():
    # extracting from feed table
    @task()
    def extract_actions():
        query_actions = """SELECT user_id,
                                  toDate(time) as event_date,
                                  age, gender, os,
                                  countIf(action = 'like') as likes,
                                  countIf(action = 'view') as views
                            FROM simulator_20220920.feed_actions
                            WHERE  event_date = today() - 1
                            GROUP BY user_id, event_date, age, gender, os
                        """
        df_actions = ph.read_clickhouse(query_actions, connection=connection)
        return df_actions
     # extracting from message table
    @task()
    def extract_messages():
        query_messages = """SELECT user_id,
                                        t1.event_date,
                                        age, gender, os,
                                        t2.messages_received,
                                        t1.messages_sent,
                                        t1.users_sent,
                                        t2.users_received
                                FROM
                                    (SELECT user_id,
                                            toDate(time) as event_date,
                                            age, gender, os,
                                            COUNT(user_id) as messages_sent,
                                            COUNT(DISTINCT user_id) as users_sent
                                    FROM simulator_20220920.message_actions
                                    WHERE  event_date = today() - 1
                                    GROUP BY user_id, event_date, age, gender, os) as t1
                                LEFT JOIN 
                                    (SELECT reciever_id,
                                            toDate(time) as event_date,
                                            COUNT(user_id) as messages_received,
                                            COUNT(DISTINCT user_id) as users_received
                                    FROM simulator_20220920.message_actions
                                    WHERE  event_date = today() - 1
                                    GROUP BY reciever_id, event_date) as t2 
                                ON t1.user_id=t2.reciever_id
                         """
        df_messages = ph.read_clickhouse(query_messages, connection=connection)
        return df_messages
    # merging booth tables
    @task()
    def merge_df(df_actions,df_messages):
        df = df_actions.merge(df_messages, how='outer', on=['user_id', 'event_date','age', 'gender','os'])
        return df
    # making a slice by the operating systems
    @task()
    def transform_os(df):
        df_os = df.groupby(['event_date','os'])\
        ['views','likes','messages_received','messages_sent','users_received','users_sent'].sum().reset_index()
        df_os['dimension'] = 'os'
        df_os.rename(columns = {'os' : 'dimension_value'}, inplace = True)
        return df_os
    # Making a slice by the gender
    @task()
    def transform_gender(df):
        df_g = df.groupby(['event_date','gender'])\
        ['views','likes','messages_received','messages_sent','users_received','users_sent'].sum().reset_index()
        df_g['dimension'] = 'gender'
        df_g.rename(columns = {'gender' : 'dimension_value'}, inplace = True)
        return df_g
    # Making a slice by the age
    @task()
    def transform_age(df):
        df_age = df.groupby(['event_date','age'])\
        ['views','likes','messages_received','messages_sent','users_received','users_sent'].sum().reset_index()
        df_age['dimension'] = 'age'
        df_age.rename(columns = {'age' : 'dimension_value'}, inplace = True)
        return df_age
    # Making a merge by all slices
    @task()
    def concat_all(df_os, df_g,df_age):
        df_total = pd.concat([df_age, df_os, df_g])
        df_total = df_total[['event_date', 'dimension', 'dimension_value', 'views',\
                             'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        df_total = df_total.astype({'event_date' : 'datetime64',
                                    'dimension' : 'str',
                                    'dimension_value' : 'str',
                                    'views' : 'int64',
                                    'likes' : 'int64',
                                    'messages_received' : 'int64',
                                    'messages_sent' : 'int64',
                                    'users_received' : 'int64',
                                    'users_sent' : 'int64'})
        return df_total
    # Uploading got table   
    @task
    def load(df_total):
        create = '''CREATE TABLE IF NOT EXISTS test.m_keller
        (event_date Date,
         dimension String,
         dimension_value String,
         views Int64,
         likes Int64,
         messages_received Int64,
         messages_sent Int64,
         users_received Int64,
         users_sent Int64
         ) ENGINE = MergeTree()
         ORDER BY event_date
         '''
        ph.execute(query=create, connection=connection_to_test)
        ph.to_clickhouse(df=df_total, table='m_keller', connection=connection_to_test, index=False)

    df_actions = extract_actions()
    df_messages = extract_messages()
    df = merge_df(df_actions,df_messages)
    df_os = transform_os(df)
    df_g = transform_gender(df)
    df_age = transform_age(df)
    df_total = concat_all(df_os, df_g,df_age)
    load(df_total)

aa_keller_hw6 = aa_keller_hw6()