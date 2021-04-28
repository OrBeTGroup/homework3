from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

 # SQL Script definition

SQL_CONTEXT = {
    'LOAD_VIEW_PAYMENT_ONE_YEAR': """
        create or replace view aermokhin.view_payment_{{ execution_date.year }} as (
          with staging as (
            with derived_columns as (
              select
                user_id,
                pay_doc_type,
                pay_doc_num,
                account,
                phone,
                billing_period,
                pay_date,
                sum,
                user_id::varchar as USER_KEY,
                account::varchar as ACCOUNT_KEY,
                billing_period::varchar as BILLING_PERIOD_KEY,
                pay_doc_type::varchat as PAY_DOC_TYPE_KEY,
                pay_doc_num::varchar as PAY_DOC_NUM_KEY,
                'PAYMENT - DATA LAKE'::varchar as RECORD_SOURCE
              from aermokhin.ods_payment
              where cast(extract('year' from cast(pay_date as timestamp)) as int) = {{ execution_date.year }}
            ),
            hashed_columns as (
              select
                user_id,
                pay_doc_type,
                account,
                phone,
                billing_period,
                pay_date,
                sum,
                USER_KEY,
                ACCOUNT_KEY,
                BILLING_PERIOD_KEY,
                PAY_DOC_TYPE_KEY,
                PAY_DOC_NUM_KEY,
                RECORD_SOURCE,
                cast((md5(nullif(upper(trim(cast(user_id as varchar))), ''))) as TEXT) as USER_PK,
                cast((md5(nullif(upper(trim(cast(account as varchar))), ''))) as TEXT) as ACCOUNT_PK,
                cast((md5(nullif(upper(trim(cast(billing_period as varchar))), ''))) as TEXT) as BILLING_PERIOD_PK,
                cast(md5(nullif(concat_ws('||',
                  coalesce(nullif(upper(trim(cast(pay_doc_type as varchar))), ''), '^^'),
                  coalesce(nullif(upper(trim(cast(pay_doc_num as varchar))), ''), '^^')
                ), '^^||^^')) as TEXT) as PAY_DOC_PK,
                cast(md5(nullif(concat_ws('||',
                  coalesce(nullif(upper(trim(cast(user_id as varchar))), ''), '^^'),
                  coalesce(nullif(upper(trim(cast(account as varchar))), ''), '^^')
                ), '^^||^^')) as TEXT) as USER_ACCOUNT_PK,
                cast(md5(nullif(concat_ws('||',
                  coalesce(nullif(upper(trim(cast(account as varchar))), ''), '^^'),
                  coalesce(nullif(upper(trim(cast(pay_doc_type as varchar))), ''), '^^'),
                  coalesce(nullif(upper(trim(cast(pay_doc_num as varchar))), ''), '^^'),
                  coalesce(nullif(upper(trim(cast(billing_period as varchar))), ''), '^^')
                ), '^^||^^||^^||^^')) as TEXT) as ACCOUNT_BILLING_PAY_PK,
                cast(md5(concat_ws('||',
                  coalesce(nullif(upper(trim(cast(phone as varchar))), ''), '^^')
                )) as TEXT) as USER_HASHDIFF, 
                cast(md5(concat_ws('||',
                  coalesce(nullif(upper(trim(cast(pay_date as varchar))), ''), '^^'),
                  coalesce(nullif(upper(trim(cast(sum as varchar))), ''), '^^')
                )) as TEXT) as PAY_DOC_HASHDIFF
              from derived_columns                                
            ),

            columns_to_select as (
              select
                user_id,
                pay_doc_type,
                pay_doc_num,
                account,
                phone,
                billing_period,
                pay_date,
                sum,
                USER_KEY,
                ACCOUNT_KEY,
                BILLING_PERIOD_KEY,
                PAY_DOC_TYPE_KEY,
                PAY_DOC_NUM_KEY,
                RECORD_SOURCE,
                USER_PK,
                ACCOUNT_PK,
                BILLING_PERIOD_PK,
                PAY_DOC_PK,
                USER_ACCOUNT_PK,
                ACCOUNT_BILLING_PAY_PK,
                USER_HASHDIFF,
                PAY_DOC_HASHDIFF,
              from hashed_columns
            )
            select * from columns_to select
          )
          
          select *,
              '{{execution_date}}'::timestamp as LOAD_DATE,
              pay_date as EFFECTIVE_FROM
          from staging
        );
    """,
    'HUBS':{
           'HUB_USER': """
                   with row_rank_1 as (
                        select * from (
                               select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE, pay_date,
                                  row_number() over (
                                     partition by USER_PK
                                     order by LOAD_DATE ASC
                                  ) as row_num
                               from aermokhin.view_payment_{{ execution_date.year }}
                         ) as h where row_num =1
                    ),
                    records_to_insert as (
                         select a.USER_PK, a.USER_KEY, a.LOAD_DATE, a.RECORD_SOURCE
                         from row_rank_1 as a
                         left join aermokhin.dds_hub_user as d
                              on a.USER_PK = d.USER_PK
                         where d.USER_PK is NULL
                    )
                    insert into aermokhin.dds_hub_user (USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE)
                    (
                         select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE
                         from records_to_insert
                    );   
           """,
           'HUB_ACCOUNT': """
                    with row_rank_1 as (
                        select * from (
                               select ACCOUNT_PK, ACCOUNT_KEY, LOAD_DATE, RECORD_SOURCE, pay_date,
                                  row_number() over (
                                     partition by ACCOUNT_PK
                                     order by LOAD_DATE ASC
                                  ) as row_num
                               from aermokhin.view_payment_{{ execution_date.year }}
                         ) as h where row_num = 1
                    ),
                    records_to_insert as (
                         select a.ACCOUNT_PK, a.ACCOUNT_KEY, a.LOAD_DATE, a.RECORD_SOURCE
                         from row_rank_1 as a
                         left join aermokhin.dds_hub_account as d
                              on a.ACCOUNT_PK = d.ACCOUNT_PK
                         where d.ACCOUNT_PK is NULL
                    )
                    insert into aermokhin.dds_hub_account (ACCOUNT_PK, ACOOUNT_KEY, LOAD_DATE, RECORD_SOURCE)
                    (
                         select ACCOUNT_PK, ACCOUNT_KEY, LOAD_DATE, RECORD_SOURCE
                         from records_to_insert
                    );   
           """,
           'HUB_BILLING_PERIOD': """
                    with row_rank_1 as (
                        select * from (
                               select BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE, pay_date,
                                  row_number() over (
                                     partition by ACCOUNT_PK
                                     order by LOAD_DATE ASC
                                  ) as row_num
                               from aermokhin.view_payment_{{ execution_date.year }}
                         ) as h where row_num = 1
                    ),
                    records_to_insert as (
                         select a.BILLING_PERIOD_PK, a.BILLING_PERIOD_KEY, a.LOAD_DATE, a.RECORD_SOURCE
                         from row_rank_1 as a
                         left join aermokhin.dds_hub_billing_period as d
                              on a.BILLING_PERIOD_PK = d.BILLING_PERIOD_PK
                         where d.BILLING_PERIOD_PK is NULL
                    )
                    insert into aermokhin.dds_hub_billing_period (BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE)
                    (
                         select BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE
                         from records_to_insert
                    );   
           """,
           'HUB_PAY_DOC': """
                    with row_rank_1 as (
                        select * from (
                               select PAY_DOC_PK, PAY_DOC__KEY, LOAD_DATE, RECORD_SOURCE, pay_date,
                                  row_number() over (
                                     partition by PAY_DOC_PK
                                     order by LOAD_DATE ASC
                                  ) as row_num
                               from aermokhin.view_payment_{{ execution_date.year }}
                         ) as h where row_num = 1
                    ),
                    records_to_insert as (
                         select a.PAY_DOC_PK, a.PAY_DOC_KEY, a.LOAD_DATE, a.RECORD_SOURCE
                         from row_rank_1 as a
                         left join aermokhin.dds_hub_pay_doc as d
                              on a.PAY_DOC_PK = d.PAY_DOC_PK
                         where d.PAY_DOC_PK is NULL
                    )
                    insert into aermokhin.dds_hub_account (PAY_DOC_PK, PAY_DOC_KEY, LOAD_DATE, RECORD_SOURCE)
                    (
                         select PAY_DOC_PK, PAY_DOC_KEY, LOAD_DATE, RECORD_SOURCE
                         from records_to_insert
                    );   
           """},

    "LINKS": {
           'LINKS_USER_ACCOUNT': """
                  with records_to_insert as (
                      select distinct
                         stg.USER_ACCOUNT_PK,
                         stg.USER_PK, stg.ACCOUNT_PK,
                         stg.LOAD_FDATE, stg,RECORD_SOURCE
                      from aermokhin.view_payment_{{ execution_date.year }} as stg
                      left join aermokhin.dds_link_user_account as tgt
                      on stg.USER_ACCOUNT_PK = tgt.USER_ACCOUNT_PK
                      where tgt.USER_ACCOUNT_PK is null
                  )
                  insert into aermokhin.dds_link_user_account (
                     USER_ACCOUNT_PK,
                     USER_PK, ACCOUNT_PK,
                     LOAD_DATE, RECORD_SOURCE)
                  (
                     select
                         USER_ACCOUNT_PK,
                         USER_PK, ACCOUNT_PK,
                         LOAD_DATE, RECORD_SOURCE
                     fron records_to_insert
                   );
           """,  
           'LINKS_ACCOUNT_BILLING_PAY': """
                  with records_to_insert as (
                      select distinct
                         stg.ACCOUNT_BILLING_PAY_PK,
                         stg.ACCOUNT_PK, stg.BILLING_PERIOD_PK, stg.PAY_DOC_PK,
                         stg.LOAD_FDATE, stg,RECORD_SOURCE
                      from aermokhin.view_payment_{{ execution_date.year }} as stg
                      left join aermokhin.dds_link_account_billing_pay as tgt
                      on stg.ACCOUNT_BILLING_PAY_PK = tgt.ACCOUNT_BILLING_PAY_PK
                      where tgt.ACCOUNT_BILLING_PAY_PK is null
                  )
                  insert into aermokhin.dds_link_account_billing_pay (
                     ACCOUNT_BILLING_PAY_PK,
                     ACCOUNT_PK, BILLING_PERIOD_PK, PAY_DOC_PK,
                     LOAD_DATE, RECORD_SOURCE)
                  (
                     select
                         ACCOUNT_BILLING_PAY_PK,
                         ACCOUNT_PK, BILLING_PERIOD_PK, PAY_DOC_PK,
                         LOAD_DATE, RECORD_SOURCE
                     fron records_to_insert
                   );
             """},
    'SATELLITES': {
           'SAT_USER_DETAILS': """
                  with source_date as (
                      select 
                         USER_PK, USER_HASHDIFF,
                         phone,
                         EFFECTIVE_FROM,
                         LOAD_DATE, RECORD_SOURCE
                      from aermokhin.view_payment_{{ execution_date.year }}
                   ),
                   update_records_as (
                      select
                         a.USER_PK, a.USER_HASHDIFF,
                         a.phone,
                         a.EFFECTIVE_FROM,
                         a.LOAD_DATE, a.RECORD_SOURCE 
                      from aermokhin.dds_sat_user_details as a
                      join source_date as b
                      on a.USER_PK = b.USER_PK
                      where a.LOAD_DATE <= b.LOAD_DATE
                   ),
                   latest_records as (
                      select * from (
                         select USER_PK, USER_HASHDIFF, LOAD_DATE,
                            case when rank() over (partition by USER_PK order by LOAD_DATE desc) = 1
                                 then 'Y'
                                 else 'N'
                            end as latest
                         from update_records
                      ) as s
                      where latest = 'Y'
                   ),
                   records_to_insert (
                      select distinct
                         e.USER_PK, USER_HASHDIFF,
                         e.phone,
                         e.EFFECTIVE_FROM,
                         e.LOAD_DATE, e.RECORD_SOURCE
                      from source_data as e
                      left join latest_records
                      on latest_records.HASHDIFF = e.HASHDIFF and
                         latest_records.USER_PK = e.USER_PK
                      where latest_records.USER_HASHDIFF is null
                    )
                    insert into aermokhin.dds_sat_user_details (
                        USER_PK, USER_HASHDIFF,
                        phone,
                        EFFECTIVE_FROM,
                        LOAD_DATE, RECORD_SOURCE)
                    (
                        select
                           USER_PK, USER_HASDIFF,
                           phone,
                           EFFECTIVE_FROM,
                           LOAD_DATE, RECORD_SOURCE
                        from records_to_insert
                     );
                """,
           'SAT_PAY_DOC_DETAILS': """
                  with source_date as (
                      select 
                         PAY_DOC_PK, PAY_DOC_HASHDIFF,
                         pay_date, sum,
                         EFFECTIVE_FROM,
                         LOAD_DATE, RECORD_SOURCE
                      from aermokhin.view_payment_{{ execution_date.year }}
                   ),
                   update_records_as (
                      select
                         a.PAY_DOC_PK, a.PAY_DOC_HASHDIFF,
                         a.pay_date, a.sum,
                         a.EFFECTIVE_FROM,
                         a.LOAD_DATE, a.RECORD_SOURCE 
                      from aermokhin.dds_sat_pay_doc_details as a
                      join source_date as b
                      on a.PAY_DOC_PK = b.PAY_DOC_PK
                      where a.LOAD_DATE <= b.LOAD_DATE
                   ),
                   latest_records as (
                      select * from (
                         select PAY_DOC_PK, PAY_DOC_HASHDIFF, LOAD_DATE,
                            case when rank() over (partition by PAY_DOC_PK order by LOAD_DATE desc) = 1
                                 then 'Y'
                                 else 'N'
                            end as latest
                         from update_records
                      ) as s
                      where latest = 'Y'
                   ),
                   records_to_insert (
                      select distinct
                         e.PAY_DOC_PK, PAY_DOC_HASHDIFF,
                         e.pay_date, a.sum,
                         e.EFFECTIVE_FROM,
                         e.LOAD_DATE, e.RECORD_SOURCE
                      from source_data as e
                      left join latest_records
                      on latest_records.HASHDIFF = e.HASHDIFF and
                         latest_records.PAY_DOC_PK = e.PAY_DOC_PK
                      where latest_records.PAY_DOC_HASHDIFF is null
                    )
                    insert into aermokhin.dds_sat_pay_doc_details (
                        PAY_DOC_PK, PAY_DOC_HASHDIFF,
                        pay_date, sum,
                        EFFECTIVE_FROM,
                        LOAD_DATE, RECORD_SOURCE)
                    (
                        select
                           PAY_DOC_PK, PAY_DOC_HASDIFF,
                           pay_date, sum,
                           EFFECTIVE_FROM,
                           LOAD_DATE, RECORD_SOURCE
                        from records_to_insert
                     );
                """},
           'DROP_VIEW_PAYMENT_ONE_YEAR': """
                drop view if exists aermokhin.view_payment_{{ execution_date.year }}
            """
}

def get_phase_context(task_phase):
    tasks = []
    for task in SQL_CONTEXT[task_phase]:
        query = SQL_CONTEXT[task_phase] [task]
        tasks.append(PostgresOperator(
            task_id = 'dds_{}_{}'.format(task_phase, task),
            dag = dag,
            sql = query
        ))
    return tasks          

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
    USERNAME + '_dwh_etl',
    default_args=default_args,
    description='DWH ETL tasks',
    schedule_interval="0 0 1 1 *",
)

view_payment_one_year = PostgresOperator(
    task_id = 'LOAD_VIEW_PAYMENT_ONE_YEAR',
    dag=dag,
    sql=SQL_CONTEXT['LOAD_VIEW_PAYMENT_ONE_YEAR']
)

for phase in ('HUBS', 'LINKS', "SATELLITES"):
    #Load HUB
    if phase == 'HUBS':
        hubs = get_phase_context(phase)
        all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)
    #Load HUB
    elif phase == 'LINKS':
        links = get_phase_context(phase)
        all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)
    #Load HUB
    elif phase == 'SATELLITES':
        satellites = get_phase_context(phase)
        all_satellites_loaded = DummyOperator(task_id="all_satellites_loaded", dag=dag)

drop_view_payment_one_year = PostgresOperator(
    task_id='DROP_VIEW_PAYMENT_ONE_YEAR',
    dag=dag, 
    sql=SQL_CONTEXT['DROP_VIEW_PAYMENT_ONE_YEAR']
)

view_payment_one_year >> hubs >> all_hubs_loaded >> links >> all_links_loaded >> satellites >> drop_view_payment_one_year