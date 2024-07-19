from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
with DAG(
    'youtube',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hello world DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['helloworld'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    get = BashOperator(
        task_id='get_yt_data',
        bash_command='date',
    )

    genre = BashOperator(
        task_id='filter_genre',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )

    country = DummyOperator(task_id='filter_country')
    task_end = DummyOperator(task_id='end')
    task_start = DummyOperator(task_id='start')
    upload = DummyOperator(task_id='upload')
    subs = DummyOperator(task_id='subs')
    views = DummyOperator(task_id='views')
    clean_music = DummyOperator(task_id='clean_music')
 
    count_genre = DummyOperator(task_id = 'count_genre')  
    avg_nums = DummyOperator(task_id = 'avg_views_subs')

    task_start >> get >> clean_music
    clean_music  >> [genre, country, upload, subs, views]
    [country, genre] >> count_genre
    [country, subs, views] >> avg_nums
 
    [avg_nums, count_genre] >> task_end
