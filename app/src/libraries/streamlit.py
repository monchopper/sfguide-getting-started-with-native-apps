import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import count_distinct,col,sum
import snowflake.permissions as permission
from sys import exit

st.set_page_config(layout="wide")
session = get_active_session()

def load_app(orders_table,site_recovery_table):
   
    if st.button('Create Network Rule'):
         df_network_func = session.sql(f"call app_instance_schema.create_network_rule()").collect()
         st.write(df_network_func)
    else:
        st.write('Goodbye')

    if st.button('Create Secret'):
         df_secret_func = session.sql(f"call app_instance_schema.create_network_secret()").collect()
         st.write(df_secret_func)
    else:
        st.write('Goodbye')

       
    if st.button('Send to Monitorial'):
         df_call_ext_func = session.sql(f"call app_instance_schema.call_monitorial_dispatch()").collect()
         st.write(df_call_ext_func)
    else:
        st.write('Goodbye')

    if st.button('Create Network Rule'):
         df_net_func = session.sql(f"call app_instance_schema.create_network_rule()").collect()
         st.write(df_net_func)
    else:
        st.write('Goodbye')

    if st.button('Create Network Policy'):
         df_net_func = session.sql(f"call app_instance_schema.create_network_policy()").collect()
         st.write(df_net_func)
    else:
        st.write('Goodbye')

    
load_app()
