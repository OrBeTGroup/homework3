from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

 # SQL Script definition

SQL_CONTEXT = {
           'DROP_VIEW_PAYMENT_ONE_YEAR': """
                drop view if exists aermokhin.fp_view_payment_{{ execution_date.year }}
                
            """
}

USERNAME = 'aermokhin'

default_args = {
    'owner': USERNAME,
    'depends_on_past': False,
    'start_date': datetime(2013, 1, 1, 0, 0, 0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=120),
}

dag = DAG(
    USERNAME + '_drop1',
    default_args=default_args,
    description='drop view_tmp',
    schedule_interval="0 0 1 1 *",
)


drop_view_payment_one_year = PostgresOperator(
    task_id='DROP_VIEW_PAYMENT_ONE_YEAR',
    dag=dag, 
    sql=SQL_CONTEXT['DROP_VIEW_PAYMENT_ONE_YEAR']
)

drop_view_payment_one_year
