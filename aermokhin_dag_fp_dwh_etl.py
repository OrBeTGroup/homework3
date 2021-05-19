from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

 # SQL Script definition

SQL_CONTEXT = {
    'LOAD_VIEWS_ONE_YEAR': """
        create or replace view aermokhin.fp_view_payment_{{ execution_date.year }} as (
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
                pay_doc_type::varchar as PAY_DOC_TYPE_KEY,
                pay_doc_num::varchar as PAY_DOC_NUM_KEY,
                'PAYMENT - GCS'::varchar as RECORD_SOURCE
              from aermokhin.ods_fp_payment
              where cast(extract('year' from cast(pay_date as timestamp)) as int) = {{ execution_date.year }}
            ),
            hashed_columns as (
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
                PAY_DOC_HASHDIFF
              from hashed_columns
            )
            select * from columns_to_select
          )
          
          select *,
              '{{execution_date}}'::timestamp as LOAD_DATE,
              pay_date as EFFECTIVE_FROM
          from staging
        );

    create or replace view aermokhin.fp_view_billing_{{ execution_date.year }} as (
          with staging as (
            with derived_columns as (
              select
                user_id,
                billing_period,
                service,
                tariff,
                sum as billing_sum,
                created_at,
                user_id::varchar as USER_KEY,
                billing_period::varchar as BILLING_PERIOD_KEY,
                service::varchar as SERVICE_KEY,
                tariff::varchar as TARIFF_KEY,
                'BILLING - GCS'::varchar as RECORD_SOURCE
              from aermokhin.ods_fp_billing
              where cast(extract('year' from cast(created_at as timestamp)) as int) = {{ execution_date.year }}
            ),
            hashed_columns as (
              select
                user_id,
                billing_period,
                service,
                tariff,
                billing_sum,
                created_at,
                USER_KEY,
                BILLING_PERIOD_KEY,
                SERVICE_KEY,
                TARIFF_KEY,
                RECORD_SOURCE,
                cast((md5(nullif(upper(trim(cast(user_id as varchar))), ''))) as TEXT) as USER_PK,
                cast((md5(nullif(upper(trim(cast(billing_period as varchar))), ''))) as TEXT) as BILLING_PERIOD_PK,
                cast((md5(nullif(upper(trim(cast(service as varchar))), ''))) as TEXT) as SERVICE_PK,               
                cast((md5(nullif(upper(trim(cast(tariff as varchar))), ''))) as TEXT) as TARIFF_PK,
                cast(md5(nullif(concat_ws('||',
                  coalesce(nullif(upper(trim(cast(user_id as varchar))), ''), '^^'),
                  coalesce(nullif(upper(trim(cast(service as varchar))), ''), '^^')
                ), '^^||^^')) as TEXT) as USER_SERVICE_PK,
               cast(md5(nullif(concat_ws('||',
                  coalesce(nullif(upper(trim(cast(service as varchar))), ''), '^^'),
                  coalesce(nullif(upper(trim(cast(tariff as varchar))), ''), '^^')
                ), '^^||^^')) as TEXT) as SERVICE_TARIFF_PK,
               cast(md5(nullif(concat_ws('||',
                  coalesce(nullif(upper(trim(cast(user_id as varchar))), ''), '^^'),
                  coalesce(nullif(upper(trim(cast(service as varchar))), ''), '^^')
                ), '^^||^^||^^||^^')) as TEXT) as SERVICE_HASHDIFF_PK,
               cast(md5(nullif(concat_ws('||',
                  coalesce(nullif(upper(trim(cast(tariff as varchar))), ''), '^^'),
                  coalesce(nullif(upper(trim(cast(service as varchar))), ''), '^^')
                ), '^^||^^||^^||^^')) as TEXT) as TARIFF_HASHDIFF_PK
             from derived_columns                                
            ),
            columns_to_select as (
              select
                user_id,
                billing_period,
                service,
                tariff,
                billing_sum,
                created_at,
                USER_KEY,
                BILLING_PERIOD_KEY,
                SERVICE_KEY,
                TARIFF_KEY,
                RECORD_SOURCE,
                USER_PK,
                BILLING_PERIOD_PK,
                SERVICE_PK,
                TARIFF_PK,
                USER_SERVICE_PK,
                SERVICE_TARIFF_PK
                SERVICE_HASHDIFF_PK,
                TARIFF_HASHDIFF_PK
              from hashed_columns
            )
            select * from columns_to_select
          )
          
          select *,
              '{{execution_date}}'::timestamp as LOAD_DATE,
              created_at as EFFECTIVE_FROM
          from staging
        );

        create or replace view aermokhin.fp_view_issue_{{ execution_date.year }} as (
          with staging as (
            with derived_columns as (
              select
                user_id,
                start_time,
                end_time,
                title,
                description,
                service,
                user_id::varchar as USER_KEY,
                start_time::varchar as START_TIME_KEY,
                service::varchar as SERVICE_KEY,
                title::varchar as ISSUE_KEY,
                'issue - GCS'::varchar as RECORD_SOURCE
              from aermokhin.ods_fp_issue
              where cast(extract('year' from cast(start_time as timestamp)) as int) = {{ execution_date.year }}
            ),
            hashed_columns as (
              select
                user_id,
                start_time,
                end_time,
                title,
                description,
                service,
                USER_KEY,
                START_TIME_KEY,
                SERVICE_KEY,
                ISSUE_KEY,
                RECORD_SOURCE,
                cast((md5(nullif(upper(trim(cast(user_id as varchar))), ''))) as TEXT) as USER_PK,
                cast(md5(nullif(concat_ws('||',
                  coalesce(nullif(upper(trim(cast(title as varchar))), ''), '^^'),
                  coalesce(nullif(upper(trim(cast(start_time as varchar))), ''), '^^')
                ), '^^||^^')) as TEXT) as ISSUE_PK,
                cast(md5(nullif(concat_ws('||',
                  coalesce(nullif(upper(trim(cast(user_id as varchar))), ''), '^^'),
                  coalesce(nullif(upper(trim(cast(title as varchar))), ''), '^^'),
                  coalesce(nullif(upper(trim(cast(start_time as varchar))), ''), '^^')
                ), '^^||^^||^^||^^')) as TEXT) as USER_ISSUE_PK,
               cast(md5(nullif(concat_ws('||',
                  coalesce(nullif(upper(trim(cast(title as varchar))), ''), '^^'),
                  coalesce(nullif(upper(trim(cast(start_time as varchar))), ''), '^^'),
                  coalesce(nullif(upper(trim(cast(service as varchar))), ''), '^^')
                ), '^^||^^||^^||^^')) as TEXT) as ISSUE_SERVICE_PK,
                cast((md5(nullif(upper(trim(cast(start_time as varchar))), ''))) as TEXT) as START_TIME_PK,
                cast((md5(nullif(upper(trim(cast(service as varchar))), ''))) as TEXT) as SERVICE_PK,
                cast(md5(nullif(concat_ws('||',
                  coalesce(nullif(upper(trim(cast(user_id as varchar))), ''), '^^'),
                  coalesce(nullif(upper(trim(cast(start_time as varchar))), ''), '^^'),
                  coalesce(nullif(upper(trim(cast(service as varchar))), ''), '^^')
                ), '^^||^^||^^||^^')) as TEXT) as ISSUE_HASHDIFF_PK
            from derived_columns                                
            ),
            columns_to_select as (
              select
                user_id,
                start_time,
                end_time,
                title,
                description,
                service,
                USER_KEY,
                START_TIME_KEY,
                SERVICE_KEY,
                ISSUE_KEY,
                RECORD_SOURCE,
                USER_PK,
                ISSUE_PK,
                USER_ISSUE_PK,
                ISSUE_SERVICE_PK,
                START_TIME_PK,
                SERVICE_PK,
                ISSUE_HASHDIFF_PK
              from hashed_columns
            )
            select * from columns_to_select
          )
          
          select *,
              '{{execution_date}}'::timestamp as LOAD_DATE,
              start_time as EFFECTIVE_FROM
          from staging
        );


       create or replace view aermokhin.fp_view_traffic_{{ execution_date.year }} as (
          with staging as (
            with derived_columns as (
              select
                user_id,
                timerequest,
                device_id,
                device_ip_addr,
                bytes_sent,
                bytes_received,
                user_id::varchar as USER_KEY,
                timerequest::varchar as TIMEREQUEST_KEY,
                device_id::varchar as DEVICE_ID_KEY,
                'traffic - GCS'::varchar as RECORD_SOURCE
              from aermokhin.ods_fp_traffic
              where cast(extract('year' from cast(timerequest as timestamp)) as int) = {{ execution_date.year }}
            ),
            hashed_columns as (
              select
                user_id,
                timerequest,
                device_id,
                device_ip_addr,
                bytes_sent,
                bytes_received,
                USER_KEY,
                TIMEREQUEST_KEY,
                DEVICE_KEY,
                RECORD_SOURCE,
                cast((md5(nullif(upper(trim(cast(user_id as varchar))), ''))) as TEXT) as USER_PK,
                cast((md5(nullif(upper(trim(cast(timerequet as varchar))), ''))) as TEXT) as TIMEREQUEST_PK,
                cast((md5(nullif(upper(trim(cast(device_id as varchar))), ''))) as TEXT) as DEVICE_PK,
                cast(md5(nullif(concat_ws('||',
                  coalesce(nullif(upper(trim(cast(user_id as varchar))), ''), '^^'),
                  coalesce(nullif(upper(trim(cast(device_id as varchar))), ''), '^^')
                ), '^^||^^')) as TEXT) as USER_DEVICE_PK,
                cast(md5(nullif(concat_ws('||',
                  coalesce(nullif(upper(trim(cast(user_id as varchar))), ''), '^^'),
                  coalesce(nullif(upper(trim(cast(device_id as varchar))), ''), '^^'),
                  coalesce(nullif(upper(trim(cast(device_ip_addr as varchar))), ''), '^^')
                ), '^^||^^||^^||^^')) as TEXT) as DEVICE_HASHDIFF_PK
            from derived_columns                                
            ),
            columns_to_select as (
              select
                user_id,
                timerequest,
                device_id,
                device_ip_addr,
                bytes_sent,
                bytes_received,
                USER_KEY,
                TIMEREQUEST_KEY,
                DEVICE_KEY,
                RECORD_SOURCE,
                USER_PK,
                TIMEREQUEST_PK,
                DEVICE_PK,
                USER_DEVICE_PK
              from hashed_columns
            )
            select * from columns_to_select
          )
          
          select *,
              '{{execution_date}}'::timestamp as LOAD_DATE,
              timerequest as EFFECTIVE_FROM
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
                               from aermokhin.fp_view_payment_{{ execution_date.year }}
                         ) as h where row_num =1
                    ),
                    records_to_insert as (
                         select a.USER_PK, a.USER_KEY, a.LOAD_DATE, a.RECORD_SOURCE
                         from row_rank_1 as a
                         left join aermokhin.dds_fp_hub_user as d
                              on a.USER_PK = d.USER_PK
                         where d.USER_PK is NULL
                    )
                    insert into aermokhin.dds_fp_hub_user (USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE)
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
                               from aermokhin.fp_view_payment_{{ execution_date.year }}
                         ) as h where row_num = 1
                    ),
                    records_to_insert as (
                         select a.ACCOUNT_PK, a.ACCOUNT_KEY, a.LOAD_DATE, a.RECORD_SOURCE
                         from row_rank_1 as a
                         left join aermokhin.dds_fp_hub_account as d
                              on a.ACCOUNT_PK = d.ACCOUNT_PK
                         where d.ACCOUNT_PK is NULL
                    )
                    insert into aermokhin.dds_fp_hub_account (ACCOUNT_PK, ACCOUNT_KEY, LOAD_DATE, RECORD_SOURCE)
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
                               from aermokhin.fp_view_payment_{{ execution_date.year }}
                         ) as h where row_num = 1
                    ),
                    records_to_insert as (
                         select a.BILLING_PERIOD_PK, a.BILLING_PERIOD_KEY, a.LOAD_DATE, a.RECORD_SOURCE
                         from row_rank_1 as a
                         left join aermokhin.dds_fp_hub_billing_period as d
                              on a.BILLING_PERIOD_PK = d.BILLING_PERIOD_PK
                         where d.BILLING_PERIOD_PK is NULL
                    )
                    insert into aermokhin.dds_fp_hub_billing_period (BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE)
                    (
                         select BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE
                         from records_to_insert
                    );   
           """,
           'HUB_PAY_DOC': """
                    with row_rank_1 as (
                        select * from (
                               select PAY_DOC_PK, PAY_DOC_TYPE_KEY, PAY_DOC_NUM_KEY, LOAD_DATE, RECORD_SOURCE, pay_date,
                                  row_number() over (
                                     partition by PAY_DOC_PK
                                     order by LOAD_DATE ASC
                                  ) as row_num
                               from aermokhin.fp_view_payment_{{ execution_date.year }}
                         ) as h where row_num = 1
                    ),
                    records_to_insert as (
                         select a.PAY_DOC_PK, a.PAY_DOC_TYPE_KEY, a.PAY_DOC_NUM_KEY, a.LOAD_DATE, a.RECORD_SOURCE
                         from row_rank_1 as a
                         left join aermokhin.dds_fp_hub_pay_doc as d
                              on a.PAY_DOC_PK = d.PAY_DOC_PK
                         where d.PAY_DOC_PK is NULL
                    )
                    insert into aermokhin.dds_fp_hub_pay_doc (PAY_DOC_PK, PAY_DOC_TYPE_KEY, PAY_DOC_NUM_KEY, LOAD_DATE, RECORD_SOURCE)
                    (
                         select PAY_DOC_PK, PAY_DOC_TYPE_KEY, PAY_DOC_NUM_KEY, LOAD_DATE, RECORD_SOURCE
                         from records_to_insert
                    );   
           """,
           'HUB_TARIFF': """
                    with row_rank_1 as (
                        select * from (
                               select TARIFF_PK, TARIFF_KEY, LOAD_DATE, RECORD_SOURCE, created_at,
                                  row_number() over (
                                     partition by TARIFF_PK
                                     order by LOAD_DATE ASC
                                  ) as row_num
                               from aermokhin.fp_view_billing_{{ execution_date.year }}
                         ) as h where row_num = 1_
                    ),
                    records_to_insert as (
                         select a.TARIFF_PK, a.TARIF_KEY, a.LOAD_DATE, a.RECORD_SOURCE
                         from row_rank_1 as a
                         left join aermokhin.dds_fp_hub_tariff as d
                              on a.TARIFF_PK = d.TARIFF_PK
                         where d.TARIFF_PK is NULL
                    )
                    insert into aermokhin.dds_fp_hub_tariff (TARIFF_PK, TARIFF_KEY, LOAD_DATE, RECORD_SOURCE)
                    (
                         select TARIFF_PK, TARIFF_KEY, LOAD_DATE, RECORD_SOURCE
                         from records_to_insert
                    );   
           """,
           'HUB_ISSUE': """
                    with row_rank_1 as (
                        select * from (
                               select ISSUE_PK, ISSUE_KEY, LOAD_DATE, RECORD_SOURCE, start_time,
                                  row_number() over (
                                     partition by ISSUE_PK
                                     order by LOAD_DATE ASC
                                  ) as row_num
                               from aermokhin.fp_view_issue_{{ execution_date.year }}
                         ) as h where row_num = 1_
                    ),
                    records_to_insert as (
                         select a.ISSUE_PK, a.ISSUE_KEY, a.LOAD_DATE, a.RECORD_SOURCE
                         from row_rank_1 as a
                         left join aermokhin.dds_fp_hub_issue as d
                              on a.ISSUE_PK = d.ISSUE_PK
                         where d.ISSUE is NULL
                    )
                    insert into aermokhin.dds_fp_hub_issue (ISSUE_PK, ISSUE_KEY, LOAD_DATE, RECORD_SOURCE)
                    (
                         select ISSUE_PK, ISSUE_KEY, LOAD_DATE, RECORD_SOURCE
                         from records_to_insert
                    );   
           """,
             'HUB_DEVICE': """
                    with row_rank_1 as (
                        select * from (
                               select DEVICE_PK, DEVICE_KEY, LOAD_DATE, RECORD_SOURCE, timerequest,
                                  row_number() over (
                                     partition by DEVICE_PK
                                     order by LOAD_DATE ASC
                                  ) as row_num
                               from aermokhin.fp_view_traffic_{{ execution_date.year }}
                         ) as h where row_num = 1_
                    ),
                    records_to_insert as (
                         select a.DEVICE_PK, a.DEVICE_KEY, a.LOAD_DATE, a.RECORD_SOURCE
                         from row_rank_1 as a
                         left join aermokhin.dds_fp_hub_device as d
                              on a.DEVICE_PK = d.DEVICE_PK
                         where d.DEVICE is NULL
                    )
                    insert into aermokhin.dds_fp_hub_device (DEVICE_PK, DEVICE_KEY, LOAD_DATE, RECORD_SOURCE)
                    (
                         select DEVICE_PK, DEVICE_KEY, LOAD_DATE, RECORD_SOURCE
                         from records_to_insert
                    );   
           """,
           'HUB_SERVICE': """
                   with row_rank_1 as (
                        select * from (
                               select SERVICE_PK, SERVICE_KEY, LOAD_DATE, RECORD_SOURCE, create_at,
                                  row_number() over (
                                     partition by USER_PK
                                     order by LOAD_DATE ASC
                                  ) as row_num
                               from aermokhin.fp_view_billing_{{ execution_date.year }}
                         ) as h where row_num =1
                    ),
                    records_to_insert as (
                         select a.SERVICE_PK, a.SERVICE_KEY, a.LOAD_DATE, a.RECORD_SOURCE
                         from row_rank_1 as a
                         left join aermokhin.dds_fp_hub_service as d
                              on a.SERVICE_PK = d.SERVICE_PK
                         where d.USER_PK is NULL
                    )
                    insert into aermokhin.dds_fp_hub_service (USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE)
                    (
                         select SERVICE_PK, SERVICE_KEY, LOAD_DATE, RECORD_SOURCE
                         from records_to_insert
                    );   
           """},

    "LINKS": {
           'LINKS_USER_SERVICE_BILLING_PERIOD': """
                  with records_to_insert as (
                      select distinct
                         stg.USER_SERVICE_BILLING_PERIOD_PK,
                         stg.USER_PK, 
                         stg.SERVICE_PK, 
                         stg.BILLING_PERIOD_PK,
                         stg.LOAD_DATE, stg.RECORD_SOURCE
                      from aermokhin.fp_view_billing_{{ execution_date.year }} as stg
                      left join aermokhin.dds_fp_link_user_service_billing_period as tgt
                      on stg.USER_SERVICE_BILLING_PERIOD_PK = tgt.USER_SERVICE_BILLING_PERIOD_PK
                      where tgt.USER_SERVICE_BILLING_PERIOD_PK is null
                  )
                  insert into aermokhin.dds_fp_link_user_service_billing_period (
                     USER_SERVICE_BILLING_PERIOD_PK,
                     USER_PK, SERVICE_PK,
                     LOAD_DATE, RECORD_SOURCE)
                  (
                     select
                         USER_SERVICE_BILLING_PERIOD_PK,
                         USER_PK, SERVICE_PK,
                         LOAD_DATE, RECORD_SOURCE
                     from records_to_insert
                   );
           """,  
           'LINKS_USER_ISSUE': """
                  with records_to_insert as (
                      select distinct
                         stg.USER_ISSUE_PK,
                         stg.USER_PK, 
                         stg.ISSUE_PK, 
                         stg.LOAD_DATE, stg.RECORD_SOURCE
                      from aermokhin.fp_view_issue_{{ execution_date.year }} as stg
                      left join aermokhin.dds_fp_link_user_issue as tgt
                      on stg.USER_ISSUE_PK = tgt.USER_ISSUE_PK
                      where tgt.USER_ISSUE_PK is null
                  )
                  insert into aermokhin.dds_fp_link_user_issue (
                     USER_ISSUE_PK,
                     USER_PK, ISSUE_PK,
                     LOAD_DATE, RECORD_SOURCE)
                  (
                     select
                         USER_ISSUE_PK,
                         USER_PK, ISSUE_PK,
                         LOAD_DATE, RECORD_SOURCE
                     from records_to_insert
                   );
           """,
           'LINKS_USER_SERVICE': """
                  with records_to_insert as (
                      select distinct
                         stg.USER_SERVICE_PK,
                         stg.USER_PK, 
                         stg.SERVICE_PK, 
                         stg.LOAD_DATE, stg.RECORD_SOURCE
                      from aermokhin.fp_view_issue_{{ execution_date.year }} as stg
                      left join aermokhin.dds_fp_link_user_service as tgt
                      on stg.USER_SERVICE_PK = tgt.USER_SERVICE_PK
                      where tgt.USER_SERVICE_PK is null
                  )
                  insert into aermokhin.dds_fp_link_user_service (
                     USER_SERVICE_PK,
                     USER_PK, SERVICE_PK,
                     LOAD_DATE, RECORD_SOURCE)
                  (
                     select
                         USER_SERVICE_PK,
                         USER_PK, SERVICE_PK,
                         LOAD_DATE, RECORD_SOURCE
                     from records_to_insert
                   );
           """,
           'LINKS_USER_SERVICE': """
                  with records_to_insert as (
                      select distinct
                         stg.USER_SERVICE_PK,
                         stg.USER_PK, 
                         stg.SERVICE_PK, 
                         stg.LOAD_DATE, stg.RECORD_SOURCE
                      from aermokhin.fp_view_issue_{{ execution_date.year }} as stg
                      left join aermokhin.dds_fp_link_user_service as tgt
                      on stg.USER_SERVICE_PK = tgt.USER_SERVICE_PK
                      where tgt.USER_SERVICE_PK is null
                  )
                  insert into aermokhin.dds_fp_link_user_service (
                     USER_SERVICE_PK,
                     USER_PK, SERVICE_PK,
                     LOAD_DATE, RECORD_SOURCE)
                  (
                     select
                         USER_SERVICE_PK,
                         USER_PK, SERVICE_PK,
                         LOAD_DATE, RECORD_SOURCE
                     from records_to_insert
                   );
           """,
           'LINKS_SERVICE_TARIFF': """
                  with records_to_insert as (
                      select distinct
                         stg.SERVICE_TARIFF_PK,
                         stg.SERVICE_PK, 
                         stg.TARIFF_PK, 
                         stg.LOAD_DATE, stg.RECORD_SOURCE
                      from aermokhin.fp_view_billing_{{ execution_date.year }} as stg
                      left join aermokhin.dds_fp_link_service_tariff as tgt
                      on stg.SERVICE_TARIFF_PK = tgt.SERVICE_TARIFF_PK
                      where tgt.SERVICE_TARIFF_PK is null
                  )
                  insert into aermokhin.dds_fp_link_service_tariff (
                     SERVICE_TARIFF_PK,
                     SERVICE_PK, TARIFF_PK,
                     LOAD_DATE, RECORD_SOURCE)
                  (
                     select
                         SERVICE_TARIFF_PK,
                         SERVICE_PK, TARIFF_PK,
                         LOAD_DATE, RECORD_SOURCE
                     from records_to_insert
                   );
           """,
           'LINKS_USER_DEVICE': """
                  with records_to_insert as (
                      select distinct
                         stg.USER_DEVICE_PK,
                         stg.USER_PK, 
                         stg.DEVICE_PK, 
                         stg.LOAD_DATE, stg.RECORD_SOURCE
                      from aermokhin.fp_view_traffic_{{ execution_date.year }} as stg
                      left join aermokhin.dds_fp_link_user_device as tgt
                      on stg.USER_DEVICE_PK = tgt.USER_DEVICE_PK
                      where tgt.USER_DEVICE_PK is null
                  )
                  insert into aermokhin.dds_fp_link_user_device (
                     USER_DEVICE_PK,
                     USER_PK, DEVICE_PK,
                     LOAD_DATE, RECORD_SOURCE)
                  (
                     select
                         USER_DEVICE_PK,
                         USER_PK, DEVICE_PK,
                         LOAD_DATE, RECORD_SOURCE
                     from records_to_insert
                   );
           """,
           'LINKS_USER_ACCOUNT': """
                  with records_to_insert as (
                      select distinct
                         stg.USER_ACCOUNT_PK,
                         stg.USER_PK, stg.ACCOUNT_PK,
                         stg.LOAD_DATE, stg.RECORD_SOURCE
                      from aermokhin.fp_view_payment_{{ execution_date.year }} as stg
                      left join aermokhin.dds_fp_link_user_account as tgt
                      on stg.USER_ACCOUNT_PK = tgt.USER_ACCOUNT_PK
                      where tgt.USER_ACCOUNT_PK is null
                  )
                  insert into aermokhin.dds_fp_link_user_account (
                     USER_ACCOUNT_PK,
                     USER_PK, ACCOUNT_PK,
                     LOAD_DATE, RECORD_SOURCE)
                  (
                     select
                         USER_ACCOUNT_PK,
                         USER_PK, ACCOUNT_PK,
                         LOAD_DATE, RECORD_SOURCE
                     from records_to_insert
                   );
           """,
           'LINKS_ACCOUNT_BILLING_PAY': """
                  with records_to_insert as (
                      select distinct
                         stg.ACCOUNT_BILLING_PAY_PK,
                         stg.ACCOUNT_PK, stg.BILLING_PERIOD_PK, stg.PAY_DOC_PK,
                         stg.LOAD_DATE, stg.RECORD_SOURCE
                      from aermokhin.fp_view_payment_{{ execution_date.year }} as stg
                      left join aermokhin.dds_fp_link_account_billing_pay as tgt
                      on stg.ACCOUNT_BILLING_PAY_PK = tgt.ACCOUNT_BILLING_PAY_PK
                      where tgt.ACCOUNT_BILLING_PAY_PK is null
                  )
                  insert into aermokhin.dds_fp_link_account_billing_pay (
                     ACCOUNT_BILLING_PAY_PK,
                     ACCOUNT_PK, BILLING_PERIOD_PK, PAY_DOC_PK,
                     LOAD_DATE, RECORD_SOURCE)
                  (
                     select
                         ACCOUNT_BILLING_PAY_PK,
                         ACCOUNT_PK, BILLING_PERIOD_PK, PAY_DOC_PK,
                         LOAD_DATE, RECORD_SOURCE
                     from records_to_insert
                   );
             """},
    'SATELLITES': {
           'SAT_SERVICE_DETAILS': """
                  with source_date as (
                      select 
                         SERVICE_PK, SERVICE_HASHDIFF,
                         created_at,
                         billing_sum,
                         EFFECTIVE_FROM,
                         LOAD_DATE, RECORD_SOURCE
                      from aermokhin.fp_view_billing_{{ execution_date.year }}
                   ),
                   update_records as (
                      select
                         a.SERVICE_PK, a.SERVICE_HASHDIFF,
                         a.created_at,
                         a.billing_sum,
                         a.EFFECTIVE_FROM,
                         a.LOAD_DATE, a.RECORD_SOURCE 
                      from aermokhin.dds_fp_sat_service_details as a
                      join source_date as b
                      on a.SERVICE_PK = b.SERVICE_PK
                      where a.LOAD_DATE <= b.LOAD_DATE
                   ),
                   latest_records as (
                      select * from (
                         select SERVICE_PK, SERVICE_HASHDIFF, LOAD_DATE,
                            case when rank() over (partition by SERVICE_PK order by LOAD_DATE desc) = 1
                                 then 'Y'
                                 else 'N'
                            end as latest
                         from update_records
                      ) as s
                      where latest = 'Y'
                   ),
                   records_to_insert as (
                      select distinct
                         e.SERVICE_PK, e.SERVICE_HASHDIFF,
                         e.created_at,
                         e.billing_sum,
                         e.EFFECTIVE_FROM,
                         e.LOAD_DATE, e.RECORD_SOURCE
                      from source_date as e
                      left join latest_records
                      on latest_records.SERVICE_HASHDIFF = e.SERVICE_HASHDIFF and
                         latest_records.SERVICE_PK = e.SERVICE_PK
                      where latest_records.SERVICE_HASHDIFF is null
                    )
                    insert into aermokhin.dds_fp_sat_service_details (
                        SERVICE_PK, SERVICE_HASHDIFF,
                        created_at, billing_sum,
                        EFFECTIVE_FROM,
                        LOAD_DATE, RECORD_SOURCE)
                    (
                        select
                           SERVICE_PK, SERVICE_HASHDIFF,
                           created_at, billing_sum,
                           EFFECTIVE_FROM,
                           LOAD_DATE, RECORD_SOURCE
                        from records_to_insert
                     );
                """,
            'SAT_ISSUE_DETAILS': """
                  with source_date as (
                      select 
                         ISSUE_PK, ISSUE_HASHDIFF,
                         START_TIME, TITLE, DESCRIPTION,
                         EFFECTIVE_FROM,
                         LOAD_DATE, RECORD_SOURCE
                      from aermokhin.fp_view_issue_{{ execution_date.year }}
                   ),
                   update_records as (
                      select
                         a.ISSUE_PK, a.ISSUE_HASHDIFF,
                         a.START_TIME, a.TITLE, a.DESCRIPTION,
                         a.EFFECTIVE_FROM,
                         a.LOAD_DATE, a.RECORD_SOURCE 
                      from aermokhin.dds_fp_sat_issue_details as a
                      join source_date as b
                      on a.ISSUE_PK = b.ISSUE_PK
                      where a.LOAD_DATE <= b.LOAD_DATE
                   ),
                   latest_records as (
                      select * from (
                         select ISSUE_PK, ISSUE_HASHDIFF, LOAD_DATE,
                            case when rank() over (partition by ISSUE_PK order by LOAD_DATE desc) = 1
                                 then 'Y'
                                 else 'N'
                            end as latest
                         from update_records
                      ) as s
                      where latest = 'Y'
                   ),
                   records_to_insert as (
                      select distinct
                         e.ISSUE_PK, e.ISSUE_HASHDIFF,
                         e.START_TIME, e.TITLE, e.DESCRIPTION,
                         e.EFFECTIVE_FROM,
                         e.LOAD_DATE, e.RECORD_SOURCE
                      from source_date as e
                      left join latest_records
                      on latest_records.ISSUE_HASHDIFF = e.ISSUE_HASHDIFF and
                         latest_records.ISSUE_PK = e.ISSUE_PK
                      where latest_records.ISSUE_HASHDIFF is null
                    )
                    insert into aermokhin.dds_fp_sat_issue_details (
                        ISSUE_PK, ISSUE_HASHDIFF,
                        START_TIME, TITLE, DESCRIPTION,
                        EFFECTIVE_FROM,
                        LOAD_DATE, RECORD_SOURCE)
                    (
                        select
                           ISSUE_PK, ISSUE_HASHDIFF,
                           START_TIME, TITLE, DESCRIPTION,
                           EFFECTIVE_FROM,
                           LOAD_DATE, RECORD_SOURCE
                        from records_to_insert
                     );
                """,

            'SAT_TARIFF_DETAILS': """
                  with source_date as (
                      select 
                         TARIFF_PK, TARIFF_HASHDIFF,
                         created_at, tariff,
                         EFFECTIVE_FROM,
                         LOAD_DATE, RECORD_SOURCE
                      from aermokhin.fp_view_billing_{{ execution_date.year }}
                   ),
                   update_records as (
                      select
                         a.TARIFF_PK, a.TARIFF_HASHDIFF,
                         a.created_at, a.tariff,
                         a.EFFECTIVE_FROM,
                         a.LOAD_DATE, a.RECORD_SOURCE 
                      from aermokhin.dds_fp_sat_tariff_details as a
                      join source_date as b
                      on a.TARIFF_PK = b.TARIFF_PK
                      where a.LOAD_DATE <= b.LOAD_DATE
                   ),
                   latest_records as (
                      select * from (
                         select TARIFF_PK, TARIFF_HASHDIFF, LOAD_DATE,
                            case when rank() over (partition by TARIFF_PK order by LOAD_DATE desc) = 1
                                 then 'Y'
                                 else 'N'
                            end as latest
                         from update_records
                      ) as s
                      where latest = 'Y'
                   ),
                   records_to_insert as (
                      select distinct
                         e.TARIFF_PK, e.TARIFF_HASHDIFF,
                         e.created_at, e.tariff,
                         e.EFFECTIVE_FROM,
                         e.LOAD_DATE, e.RECORD_SOURCE
                      from source_date as e
                      left join latest_records
                      on latest_records.TARIFF_HASHDIFF = e.TARIFF_HASHDIFF and
                         latest_records.TARIFF_PK = e.TARIFF_PK
                      where latest_records.TARIFF_HASHDIFF is null
                    )
                    insert into aermokhin.dds_fp_sat_tariff_details (
                        TARIFF_PK, TARIFF_HASHDIFF,
                        created_at, tariff,
                        EFFECTIVE_FROM,
                        LOAD_DATE, RECORD_SOURCE)
                    (
                        select
                           TARIFF_PK, TARIFF_HASHDIFF,
                           created_at, tariff,
                           EFFECTIVE_FROM,
                           LOAD_DATE, RECORD_SOURCE
                        from records_to_insert
                     );
                """,
           'SAT_DEVICE_DETAILS': """
                  with source_date as (
                      select 
                         DEVICE_PK, DEVICE_HASHDIFF,
                         timerequest, device_id, device_ip_addr,
                         bytes_send, bytes_received,
                         EFFECTIVE_FROM,
                         LOAD_DATE, RECORD_SOURCE
                      from aermokhin.fp_view_traffic_{{ execution_date.year }}
                   ),
                   update_records as (
                      select
                         a.DEVICE_PK, a.DEVICE_HASHDIFF,
                         a.timerequest, a.device_id, e.device_ip_addr,
                         a.bytes_send, a.bytes_received,
                         a.EFFECTIVE_FROM,
                         a.LOAD_DATE, a.RECORD_SOURCE 
                      from aermokhin.dds_fp_sat_device_details as a
                      join source_date as b
                      on a.DEVICE_PK = b.DEVICE_PK
                      where a.LOAD_DATE <= b.LOAD_DATE
                   ),
                   latest_records as (
                      select * from (
                         select DEVICE_PK, DEVICE_HASHDIFF, LOAD_DATE,
                            case when rank() over (partition by DEVICE_PK order by LOAD_DATE desc) = 1
                                 then 'Y'
                                 else 'N'
                            end as latest
                         from update_records
                      ) as s
                      where latest = 'Y'
                   ),
                   records_to_insert as (
                      select distinct
                         e.DEVICE_PK, e.DEVICE_HASHDIFF,
                         e.timerequest, e.device_id, e.device_ip_addr,
                         e.bytes_sent, e.bytes_received,
                         e.EFFECTIVE_FROM,
                         e.LOAD_DATE, e.RECORD_SOURCE
                      from source_date as e
                      left join latest_records
                      on latest_records.DEVICE_HASHDIFF = e.DEVICE_HASHDIFF and
                         latest_records.DEVICE_PK = e.DEVICE_PK
                      where latest_records.DEVICE_HASHDIFF is null
                    )
                    insert into aermokhin.dds_fp_sat_device_details (
                        DEVICE_PK, DEVICE_HASHDIFF,
                        timerequest, device_id, device_ip_addr,
                        bytes_sent, bytes_received,
                        EFFECTIVE_FROM,
                        LOAD_DATE, RECORD_SOURCE)
                    (
                        select
                           DEVICE_PK, DEVICE_HASHDIFF,
                           timerequest, device_id, device_ip_addr,
                           bytes_sent, bytes_received,
                           EFFECTIVE_FROM,
                           LOAD_DATE, RECORD_SOURCE
                        from records_to_insert
                     );
                """,


           'SAT_USER_DETAILS': """
                  with source_date as (
                      select 
                         USER_PK, USER_HASHDIFF,
                         phone,
                         EFFECTIVE_FROM,
                         LOAD_DATE, RECORD_SOURCE
                      from aermokhin.fp_view_payment_{{ execution_date.year }}
                   ),
                   update_records as (
                      select
                         a.USER_PK, a.USER_HASHDIFF,
                         a.phone,
                         a.EFFECTIVE_FROM,
                         a.LOAD_DATE, a.RECORD_SOURCE 
                      from aermokhin.dds_fp_sat_user_details as a
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
                   records_to_insert as (
                      select distinct
                         e.USER_PK, e.USER_HASHDIFF,
                         e.phone,
                         e.EFFECTIVE_FROM,
                         e.LOAD_DATE, e.RECORD_SOURCE
                      from source_date as e
                      left join latest_records
                      on latest_records.USER_HASHDIFF = e.USER_HASHDIFF and
                         latest_records.USER_PK = e.USER_PK
                      where latest_records.USER_HASHDIFF is null
                    )
                    insert into aermokhin.dds_fp_sat_user_details (
                        USER_PK, USER_HASHDIFF,
                        phone,
                        EFFECTIVE_FROM,
                        LOAD_DATE, RECORD_SOURCE)
                    (
                        select
                           USER_PK, USER_HASHDIFF,
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
                      from aermokhin.fp_view_payment_{{ execution_date.year }}
                   ),
                   update_records as (
                      select
                         a.PAY_DOC_PK, a.PAY_DOC_HASHDIFF,
                         a.pay_date, a.sum,
                         a.EFFECTIVE_FROM,
                         a.LOAD_DATE, a.RECORD_SOURCE 
                      from aermokhin.dds_fp_sat_pay_doc_details as a
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
                   records_to_insert as (
                      select distinct
                         e.PAY_DOC_PK, e.PAY_DOC_HASHDIFF,
                         e.pay_date, e.sum,
                         e.EFFECTIVE_FROM,
                         e.LOAD_DATE, e.RECORD_SOURCE
                      from source_date as e
                      left join latest_records
                      on latest_records.PAY_DOC_HASHDIFF = e.PAY_DOC_HASHDIFF and
                         latest_records.PAY_DOC_PK = e.PAY_DOC_PK
                      where latest_records.PAY_DOC_HASHDIFF is null
                    )
                    insert into aermokhin.dds_fp_sat_pay_doc_details (
                        PAY_DOC_PK, PAY_DOC_HASHDIFF,
                        pay_date, sum,
                        EFFECTIVE_FROM,
                        LOAD_DATE, RECORD_SOURCE)
                    (
                        select
                           PAY_DOC_PK, PAY_DOC_HASHDIFF,
                           pay_date, sum,
                           EFFECTIVE_FROM,
                           LOAD_DATE, RECORD_SOURCE
                        from records_to_insert
                     );
                """},
           'DROP_VIEW_ONE_YEAR': """
                drop view if exists aermokhin.fp_view_payment_{{ execution_date.year }};
                drop view if exists aermokhin.fp_view_billing_{{ execution_date.year }};
                drop view if exists aermokhin.fp_view_issue_{{ execution_date.year }};
                drop view if exists aermokhin.fp_view_traffic_{{ execution_date.year }};
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
    USERNAME + '_dwh_fp_etl',
    default_args=default_args,
    description='DWH ETL tasks',
    schedule_interval="0 0 1 1 *",
)

view_one_year = PostgresOperator(
    task_id = 'LOAD_VIEWS_ONE_YEAR',
    dag=dag,
    sql=SQL_CONTEXT['LOAD_VIEWS_ONE_YEAR']
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

drop_view_one_year = PostgresOperator(
    task_id='DROP_VIEW_ONE_YEAR',
    dag=dag, 
    sql=SQL_CONTEXT['DROP_VIEW_ONE_YEAR']
)

view_one_year >> hubs >> all_hubs_loaded >> links >> all_links_loaded >> satellites >> all_satellites_loaded >> drop_view_one_year
