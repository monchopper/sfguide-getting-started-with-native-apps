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


    st.code(f"""
        -- Step 6.  use role ACCOUNTADMIN
        GRANT CREATE DATABASE ON ACCOUNT TO APPLICATION MONITORIAL_APP_2;
        GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION MONITORIAL_APP_2;
        """,language='sql')
    

    statements = [
        f"CREATE DATABASE IF NOT EXISTS monitorial_db  ",
        f"GRANT USAGE on DATABASE monitorial_db to application role monitorial_admin ",
        f"CREATE SCHEMA monitorial_db.custom_monitors ",
        f"GRANT USAGE ON SCHEMA monitorial_db.custom_monitors to application role monitorial_admin ",
    ]

    

    if st.button('Step 7.  Create database'):
        for statement in statements:
            try:
                session.sql(statement).collect()
            except Exception as e:
                st.write(e)
                exit(1)
    else:
        st.write('Monitorial Database Creation Failed')

load_app()

    

