import streamlit as st
import pandas as pd
import altair as alt
import snowflake.snowpark as snowpark
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import count_distinct,col,sum,lit,object_construct,any_value
import snowflake.permissions as permissions
from sys import exit


st.set_page_config(layout="wide")
session = get_active_session()

def load_app():

    df_get_account_details = session.sql(f"select CURRENT_ORGANIZATION_NAME() as organization_name,current_account_name() AS account_name,current_account() AS account_locator,current_region() AS account_region").collect()
    st.write(df_get_account_details)
   
    df_network_func = session.sql(f"call monitorial_config.create_network_rule()").collect()
    st.write(df_network_func)
    st.write('Network Rule created')

    df_create_database = session.sql(f"call monitorial_config.deploy_setup_monitorial_db_and_assest_creation_sproc()").collect()
    st.write(df_create_database)
    st.write('Monitorial database and sprocs not created')

    st.code(f"""
        -- Copy this code and run it in Snowsight under the accountadmin role
        use role accountadmin;
        grant ownership on procedure  montorial_db.monitorial_assets.run_setup_external_access_integrations() TO ROLE accountadmin;
        call montorial_db.monitorial_assets.run_setup_external_access_integrations();
        """,language='sql')
    
    #as of now.  The External network access has been created.  Need to call the func that ( the aws stuff and create the 
    #notification integration


    
    if st.button('Create Get ARN-AWS details from Monitorial'):
        df_create_get_monitorial_aws_arn = session.sql(f"call create_monitorial_get_aws_arn()").collect()
        st.write(df_create_get_monitorial_aws_arn)
    else:
        st.write('Monitorial get aws arn func failed')
    
    st.code(f"""
        EXECUTE IMMEDIATE $$
        DECLARE 
            res RESULTSET DEFAULT (DESCRIBE INTEGRATION MONITORIAL_ERROR_INTEGRATION);
            query varchar default 'select object_construct(*):property_value::string as sf_aws_external_id from table(result_scan(last_query_id())) where "property" = ?';
                query_create_not_int default 'create notification integration if not exists MONITORIAL_ERROR_INTEGRATION
                enabled = true
                type = QUEUE
                direction = OUTBOUND
                notification_provider = AWS_SNS
                aws_sns_topic_arn = "arn:aws:sns:ap-southeast-2:415570042924:2d59d83f-5855-4a53-b891-a843bf558cbc-notifications"
                aws_sns_role_arn = "arn:aws:iam::415570042924:role/2d59d83f-5855-4a53-b891-a843bf558cbc-role-sns"';
            notification_propery varchar default 'SF_AWS_EXTERNAL_ID';
            res_integrations resultset;
        BEGIN 
            EXECUTE IMMEDIATE query_create_not_int;
            DESC INTEGRATION MONITORIAL_ERROR_INTEGRATION;
            res_integrations := (EXECUTE IMMEDIATE :query USING (notification_propery));
        RETURN table(res_integrations);
        END;
        $$
        ;
""",language='sql')

   

    if st.button('Step 3.  Create Monitorial Dispatch Function'):
         df_dispatch_func = session.sql(f"call monitorial_config.create_monitorial_dispatch_func()").collect()
         st.write(df_dispatch_func)
    else:
        st.write('Monitorial Dispatch Function not created')

    if st.button('Step 88 Show Tasks'):
         df_show_tasks = session.sql(f"show tasks in account").collect()
         st.write(df_show_tasks)
    else:
        st.write('Cannot show tasks')

    """
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

    if st.button('Test create warehouse for tasks.'):
         df_create_warehouse = session.sql(f"call monitorial_config.create_task_warehouse()").collect()
         st.write(df_create_warehouse)
    else:
        st.write('Test create warehouse for tasks') 

    if st.button('Test Creating Monitorial like Task'):
         df_call_dispatch_func = session.sql(f"call monitorial_config.create_task_test()").collect()
         st.write(df_call_dispatch_func)
    else:
        st.write('Test create task')

    if st.button('Step Test Task'):
         df = session.sql('''EXECUTE TASK monitorial_assets.t1''').collect()
         st.write(df)
    else:
        st.write('Test task works')

    """

load_app()

if not permissions.get_held_account_privileges(["CREATE DATABASE"]):
    st.error("The app needs CREATE DB privilege to replicate data")

    

