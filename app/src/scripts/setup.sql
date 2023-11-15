-- ==========================================
-- This script runs when the app is installed 
-- ==========================================

-- Create Application Role and Schema
create application role if not exists monitorial_admin;
create or alter versioned schema monitorial_config;
create or replace schema monitorial_assets;

CREATE TABLE IF NOT EXISTS monitorial_assets.Configs (
   setting object
);



create or replace procedure monitorial_config.create_monitorial_get_aws_arn()
returns string
language sql
AS '
BEGIN
CREATE OR REPLACE FUNCTION get_monitorial_get_aws_arn()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.9
HANDLER = ''retrieve_arn_from_aws''
PACKAGES = (''snowflake-snowpark-python'',''requests'')
AS
$$
import _snowflake
import json
import requests
import uuid

session = requests.Session()

def retrieve_arn_from_aws():
   return ["arn:aws:sns:ap-southeast-2:415570042924:2d59d83f-5855-4a53-b891-a843bf558cbc-notifications|arn:aws:iam::415570042924:role/2d59d83f-5855-4a53-b891-a843bf558cbc-role-sns"]

$$;
END;
';


create or replace procedure monitorial_config.deploy_setup_monitorial_db_and_assest_creation_sproc()
returns string
language sql
AS '
BEGIN
   create or replace database montorial_db;
   create or replace schema montorial_db.monitorial_assets;
   grant usage on database montorial_db to application role monitorial_admin;
   grant usage on schema montorial_db.monitorial_assets to application role monitorial_admin;
   CREATE OR REPLACE PROCEDURE montorial_db.monitorial_assets.run_setup_external_access_integrations()
    RETURNS VARCHAR(16777216)
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
    AS
    $$
    try {
    
            var external_network_access_query = `
            CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION monitorial_access_integration
            ALLOWED_NETWORK_RULES = (monitorial_app_2.monitorial_config.monitorial_service_network_rule)
            ENABLED = true;`
            snowflake.createStatement({ sqlText: external_network_access_query }).execute();   

            var grant_to_external_network_access_query = `
            GRANT USAGE ON INTEGRATION monitorial_access_integration TO APPLICATION MONITORIAL_APP_2;`
            snowflake.createStatement({ sqlText: grant_to_external_network_access_query }).execute();  
    
    } catch (err) {

        result = `Failed Caller: Code: ${err.code} \\n  State: ${err.state}`;
        result += `\\n  Message: ` + err.message;
        result += `\\nStack Trace:\\n` + err.stackTraceTxt;
        return result;
    }
    return ''Done'';
    $$;
END;
';
create or replace streamlit monitorial_config.streamlit from '/libraries' main_file='streamlit.py';

create or replace procedure monitorial_config.create_network_rule()
returns string
language sql
AS $$
BEGIN
   CREATE OR REPLACE NETWORK RULE monitorial_service_network_rule
      TYPE = HOST_PORT
      VALUE_LIST = ('de-monitorial-dev-ae-apim.azure-api.net') 
      MODE= EGRESS;
   grant usage on network rule monitorial_service_network_rule to application role monitorial_admin;
   RETURN 'created network Rule';
END;
$$;

create or replace procedure monitorial_config.create_monitorial_dispatch_func()
returns string
language sql
AS '
BEGIN
CREATE OR REPLACE FUNCTION monitorial_dispatch(object_name string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.9
HANDLER = ''send_notification''
EXTERNAL_ACCESS_INTEGRATIONS = (monitorial_access_integration)
PACKAGES = (''snowflake-snowpark-python'',''requests'')
AS
$$
import _snowflake
import json
import requests
import uuid
from typing import List
from datetime import datetime

session = requests.Session()

def to_object(object_name: str):
    data = {}
    data["messageId"] = str(uuid.uuid4())
    data["timestamp"] = str(datetime.now())
    data["objectName"] = object_name
    data["environment"] = "Production"
    data["messageType"] = "Security"
    data["severity"] = "critical"
    data["description"] = "From Snowflake External Network Access Python Function"
    data["messages"] = [{"load_time": "08/05/2006 03:05:15 PM", "message": "External Network Access"}]
    return data

def to_json(object_name: str):
    return json.dumps(to_object(object_name), default=lambda o: o.__dict__, sort_keys=True, indent=4)

def send_notification(object_name: str):
    url = ''https://de-monitorial-dev-ae-apim.azure-api.net/monitorial/v1/4c74a0fa-8eda-45c6-a8cf-143b9dcfe6a8/webhook?subscription-key=f0d104566df647309b71310ce0ebfbc1''
    headers = {''Content-type'': ''application/json'', ''Accept'': ''application/json''}
    response = requests.post(url, data=to_json(object_name), headers=headers)
    if response.status_code == 200:
        return [response.json()["data"][0][1]]
    else:
        return ["Failed to send notification"]
$$;
END;
';
create or replace procedure monitorial_config.grant_func_privileges()
returns string
language sql
AS $$
BEGIN
   GRANT USAGE ON FUNCTION monitorial_dispatch(STRING) to application role monitorial_admin;
   GRANT USAGE ON FUNCTION get_monitorial_get_aws_arn() to application role monitorial_admin;
   RETURN 'Granted monitorial_dispatch function to monitorial_admin role';
END;
$$;
create or replace procedure monitorial_config.create_task_warehouse()
returns string
language sql
AS $$
BEGIN
   CREATE OR REPLACE WAREHOUSE p_task_wh WAREHOUSE_SIZE=XSMALL INITIALLY_SUSPENDED=TRUE auto_suspend = 60 auto_resume = true;
   grant usage on warehouse p_task_wh to application role monitorial_admin;
   grant operate on warehouse p_task_wh to application role monitorial_admin;
   RETURN 'created warehouse';
END;
$$;
create or replace procedure monitorial_config.create_task_test()
returns string
language sql
AS $$
BEGIN
   CREATE TASK monitorial_assets.t1
    SCHEDULE = '1 minute'
    WAREHOUSE = 'p_task_wh'
    AS
    select monitorial_config.monitorial_dispatch('shit_head');
    alter task monitorial_assets.t1 resume;
   RETURN 'Task created';
END;
$$;

create or replace procedure monitorial_config.update_reference(ref_name string, operation string, ref_or_alias string)
returns string
language sql
as $$
begin
  case (operation)
    when 'ADD' then
       select system$set_reference(:ref_name, :ref_or_alias);
    when 'REMOVE' then
       select system$remove_reference(:ref_name, :ref_or_alias);
    when 'CLEAR' then
       select system$remove_all_references();
    else
       return 'Unknown operation: ' || operation;
  end case;
  return 'Success';
end;
$$;


-- Grant usage and permissions on objects
grant usage on schema monitorial_config to application role monitorial_admin;
grant usage on schema monitorial_assets to application role monitorial_admin;
grant create secret on schema monitorial_config to application role monitorial_admin;
grant create network rule on schema monitorial_config to application role monitorial_admin;
grant usage on streamlit monitorial_config.streamlit to application role monitorial_admin;
grant usage on procedure monitorial_config.create_network_rule() to application role monitorial_admin;
grant usage on procedure monitorial_config.create_monitorial_dispatch_func() to application role monitorial_admin;
grant usage on procedure monitorial_config.grant_func_privileges() to application role monitorial_admin;
grant usage on procedure monitorial_config.create_task_test() to application role monitorial_admin;
grant usage on procedure monitorial_config.create_monitorial_get_aws_arn() to application role monitorial_admin;
grant usage on procedure monitorial_config.deploy_setup_monitorial_db_and_assest_creation_sproc() to application role monitorial_admin;
grant usage on procedure monitorial_config.create_task_warehouse() to application role monitorial_admin;
grant create task on schema monitorial_config to application role monitorial_admin;
GRANT SELECT ON TABLE monitorial_assets.Configs TO APPLICATION ROLE monitorial_admin;

--grant usage on warehouse p_task_wh to application role monitorial_admin;
--grant operate on warehouse p_task_wh to application role monitorial_admin;