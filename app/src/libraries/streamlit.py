import streamlit as st
import pandas as pd
import altair as alt
import snowflake.snowpark as snowpark
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import count_distinct,col,sum,lit,object_construct
import snowflake.permissions as permission


from sys import exit

class JdbcDataFrameReader:
    def __init__(self):
        self.options = {}
    def option(self,key:str,value:str):
        self.options[lit(key)] = lit(value)
        return self
    def query(self,sql:str):
        self.query_stmt = lit(sql)
        return self
    def load(self):
        session = get_active_session()
        jdbc_options = object_construct(*[item for pair in self.options.items() for item in pair])
        return session.table_function("READ_JDBC_NATIVE",jdbc_options,self.query_stmt)
def format(self,format_name):
        return JdbcDataFrameReader() if format_name == "jdbc" else Exception("not supported")
snowpark.DataFrameReader.format = format


st.set_page_config(layout="wide")
session = get_active_session()

def load_app():
   
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

    if st.button('Create JDBC Function'):
         df_jdbc_func = session.sql(f"call app_instance_schema.create_read_jdbc()").collect()
         st.write(df_jdbc_func)
    else:
        st.write('Goodbye')

    if st.button('run query from sql server'):
         load_data(session=session)


def load_data(session: snowpark.Session):
    df_lsn = session.read\
    .format("jdbc")\
    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
    .option("url","jdbc:sqlserver://omnata-sandpit.database.windows.net:1433;database=omnata-src")\
    .option("use_secrets","true")\
    .query("SELECT * FROM [cdc].[SalesLT_Address_CT]")\
    .load()
    df_lsn.save_as_table("cdc_data", table_type="transient")
    st.table(get_active_session().sql(f"select * from cdc_data").collect())

    

load_app()

    

