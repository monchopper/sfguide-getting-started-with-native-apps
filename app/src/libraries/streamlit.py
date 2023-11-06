import streamlit as st
import pandas as pd
import altair as alt
import snowflake.snowpark as snowpark
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import count_distinct,col,sum,lit,object_construct,any_value
import snowflake.permissions as permission
from sys import exit


st.set_page_config(layout="wide")
session = get_active_session()

def load_app():
   
    if st.button('Step 1.  Create Network Rule'):
         df_network_func = session.sql(f"call monitorial_config.create_network_rule()").collect()
         st.write(df_network_func)
    else:
        st.write('Network Rule not created')


    st.code(f"""
        -- Step 2.  use role ACCOUNTADMIN
        CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION monitorial_access_integration
        ALLOWED_NETWORK_RULES = (monitorial_app.monitorial_config.monitorial_service_network_rule)
        ENABLED = true;
        GRANT USAGE ON INTEGRATION monitorial_access_integration TO APPLICATION MONITORIAL_APP_2;
        """,language='sql')
    
    
    if st.button('Step 3.  Create Monitorial Dispatch Function'):
         df_dispatch_func = session.sql(f"call monitorial_config.create_monitorial_dispatch_func()").collect()
         st.write(df_dispatch_func)
    else:
        st.write('Monitorial Dispatch Function not created')


    if st.button('Step 4.  Grant Usage on Monitorial Dispatch Function'):
         df_dispatch_func_grant = session.sql(f"call monitorial_config.grant_monitorial_dispatch_func()").collect()
         st.write(df_dispatch_func_grant)
    else:
        st.write('Monitorial Dispatch Function not granted')

    
    if st.button('Step 5.  Test Monitorial Dispatch Function'):
         df_call_dispatch_func = session.sql(f"select monitorial_config.monitorial_dispatch('shit_head')").collect()
         st.write(df_call_dispatch_func)
    else:
        st.write('Monitorial Dispatch Function Failed')

    if st.button('Step test create warehouse for tasks.'):
         df_create_warehouse = session.sql(f"call monitorial_config.create_task_warehouse()").collect()
         st.write(df_create_warehouse)
    else:
        st.write('Monitorial Dispatch Function Failed') 

    if st.button('Step test task creation.  Test Creating Task'):
         df_call_dispatch_func = session.sql(f"call monitorial_config.create_task_test()").collect()
         st.write(df_call_dispatch_func)
    else:
        st.write('Monitorial Dispatch Function Failed')

    if st.button('Step Manually Execute Task'):
         df = session.sql('''EXECUTE TASK monitorial_assets.t1''').collect()
         st.write(df)
    else:
        st.write('Monitorial Dispatch Function Failed')

    if st.button('Step test Get Snowflake details'):
         df_call_account_details = session.sql(f"select CURRENT_ORGANIZATION_NAME() as organization_name,current_account_name() AS account_name,current_account() AS account_locator,current_region() AS account_region").collect()
         st.write(df_call_account_details)
    else:
        st.write('Monitorial Dispatch Function Failed')


    st.code(f"""
        -- Step 6.  use role ACCOUNTADMIN
        create notification integration if not exists MONITORIAL_ERROR_INTEGRATION
            enabled = true
            type = QUEUE
            direction = OUTBOUND
            notification_provider = AWS_SNS
            aws_sns_topic_arn = 'arn:aws:sns:ap-southeast-2:415570042924:2d59d83f-5855-4a53-b891-a843bf558cbc-notifications'
            aws_sns_role_arn = 'arn:aws:iam::415570042924:role/2d59d83f-5855-4a53-b891-a843bf558cbc-role-sns';
        grant usage on integration MONITORIAL_ERROR_INTEGRATION to role SYSADMIN;
        grant usage on integration MONITORIAL_ERROR_INTEGRATION TO APPLICATION MONITORIAL_APP_2;
        """,language='sql')
    
    if st.button('Step Get Notification Details. '):
         df = session.sql('''desc integration MONITORIAL_ERROR_INTEGRATION''').collect()
         st.write(df)
    else:
        st.write('Monitorial Dispatch Function Failed')

load_app()

    

