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

if permissions.get_missing_account_privileges(
    ["CREATE DATABASE", "EXECUTE TASK", "EXECUTE MANAGED TASK","CREATE WAREHOUSE","MANAGE WAREHOUSES"]
):
    permissions.request_account_privileges(
        ["CREATE DATABASE", "EXECUTE TASK", "EXECUTE MANAGED TASK","CREATE WAREHOUSE","MANAGE WAREHOUSES"]
    )

def load_app():

    company_name = st.text_input('Enter your company name')
    given_name = st.text_input('Enter your Given Name')
    surname = st.text_input('Enter your Surname')
    email = st.text_input('Enter your Email Address')
    country = st.text_input('Enter your Country')

    df_get_account_details = session.sql(f"select CURRENT_ORGANIZATION_NAME() as organization_name,current_account_name() AS account_name,current_account() AS account_locator,lower(current_region()) AS account_region, current_database() as app_database").collect()
    #pd_get_account_details = df_get_account_details.to_pandas()
    monitorial_database_name = df_get_account_details[0][4]
    snowflake_organisation = df_get_account_details[0][0]
    snowflake_account = df_get_account_details[0][1]
    snowflake_locator = df_get_account_details[0][2]
    snowflake_region = df_get_account_details[0][3]

    create_external_access_sql = """CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION monitorial_access_integration
            ALLOWED_NETWORK_RULES = (""" + monitorial_database_name + """.monitorial_config.monitorial_service_network_rule)
            ENABLED = true;
            GRANT USAGE ON INTEGRATION monitorial_access_integration TO APPLICATION MONITORIAL_APP_2;"""

   
    df_network_func = session.sql(f"call monitorial_config.deploy_network_rule()").collect()
    st.write(df_network_func)
    st.write('Network Rule created')

    df_create_database = session.sql(f"call monitorial_config.deploy_setup_monitorial_db_and_assest_creation_sproc()").collect()
    st.write(df_create_database)
    st.write('Monitorial database and sprocs not created')

    st.code(create_external_access_sql,language='sql')

    #st.code(f"""
    #    -- Copy this code and run it in Snowsight under the accountadmin role
    #    use role accountadmin;
    #    grant ownership on procedure  montorial_db.monitorial_assets.run_setup_external_access_integrations() TO ROLE accountadmin;
    #    call montorial_db.monitorial_assets.run_setup_external_access_integrations();
    #    """,language='sql')
    
    #as of now.  The External network access has been created.  Need to call the func that ( the aws stuff and create the 
    #notification integration


    
    if st.button('Create Monitorial Sign Up Function'):
        df_create_get_monitorial_aws_arn = session.sql(f"call deploy_monitorial_sign_up()").collect()
        st.write(df_create_get_monitorial_aws_arn)
    else:
        st.write( 'Monitorial Sign Up Function - Failed')

    if st.button('Create Monitorial Dispatch Function'):
         df_dispatch_func = session.sql(f"call deploy_monitorial_dispatch_func()").collect()
         
         st.write(df_dispatch_func)
    else:
        st.write('Monitorial Dispatch Function not created')

    if st.button('Grant rights to func'):
        df_func_rights = session.sql(f"call grant_func_privileges()").collect()
        st.write(df_func_rights)
    else:
        st.write('Monitorial funcs not granted')

    if st.button('Step 5 - Test Monitorial Dispatch Function'):
         df_test_monitorial_dispatch = session.sql(f"select MONITORIAL_CONFIG.MONITORIAL_DISPATCH('choremomma')").collect()
         st.write(df_test_monitorial_dispatch)
    else:
        st.write('Test Monitorial Dispatch Function Failed')

# Adapt this to grab from Monitorial Cloud
        

    create_external_access_sql = """insert into monitorial_assets.Configs (name, setting) select 'cloud_keys', 
    MONITORIAL_CONFIG.monitorial_sign_up('""" + company_name + """','""" + email + """','""" + given_name + """','""" + surname + """','""" + snowflake_region.lower() + """','""" + snowflake_organisation + """','""" + snowflake_account + """','""" + snowflake_locator + """','""" + country + """')::variant"""
    
    st.code(create_external_access_sql,language='sql')
    
    if st.button('Register with Monitorial Cloud'):
       
        df_get_monitorial_endpoints = session.sql(create_external_access_sql).collect()
        
        st.write(df_get_monitorial_endpoints)
    else:
        st.write('Register with Monitorial Cloud - Failed')

    if st.button('Show Confis table'):
        df_get_configs_table = session.sql(f"select * from monitorial_assets.configs").collect()
        
        st.write(df_get_configs_table)
    else:
        st.write('Retrieving data from configs table - Failed')
    
    if st.button('Show cloud endpoints view'):
        df_get_cloud_endpoints = session.sql(f"select * from monitorial_assets.cloud_endpoint").collect()
        
        st.write(df_get_cloud_endpoints)
    else:
        st.write('Retrieving data from configs table - Failed')

    aws_sns_topic_arn = df_get_cloud_endpoints[0][14]
    aws_sns_role_arn = df_get_cloud_endpoints[0][8]

    st.code(f"""
        create notification integration if not exists MONITORIAL_ERROR_INTEGRATION
            enabled = true
            type = QUEUE
            direction = OUTBOUND
            notification_provider = AWS_SNS
            aws_sns_topic_arn = '""" + aws_sns_topic_arn + """'
            aws_sns_role_arn = '""" + aws_sns_role_arn + """';
        GRANT USAGE ON INTEGRATION MONITORIAL_ERROR_INTEGRATION TO APPLICATION MONITORIAL_APP_2;
        """,language='sql')



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

   

    

    sf_aws_external_id = st.text_input('Enter the SF_AWS_EXTERNAL_ID from the above query:')

   
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
         df_create_warehouse = session.sql(f"call monitorial_config.deploy_task_warehouse()").collect()
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



    

