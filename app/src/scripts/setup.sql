-- ==========================================
-- This script runs when the app is installed 
-- ==========================================

-- Create Application Role and Schema
create application role if not exists monitorial_admin;
create or alter versioned schema monitorial_config;
create or replace schema monitorial_assets;

CREATE TABLE IF NOT EXISTS monitorial_assets.Configs (
   name varchar,
   setting variant
);

CREATE VIEW IF NOT EXISTS monitorial_assets.cloud_endpoint AS
with base_parse_json as (
    select 
        parse_json(setting) as payload 
    from 
        configs 
    where 
        name = 'cloud_keys' 
)
select 
    payload:customer_id::string as customer_id,
    payload:aws.apiIntegration.aws_external_id::string as apiIntegration_aws_external_id,
    payload:aws.apiIntegration.configuration_complete::string as apiIntegration_configuration_complete,
    payload:aws.apiIntegration.iam_role_arn::string as apiIntegration_iam_role_arn,
    payload:aws.apiIntegration.iam_user_arn::string as apiIntegration_iam_user_arn,
    payload:aws.apiIntegration.provisioning_requested::string as apiIntegration_provisioning_requested,
    payload:aws.notificationIntegration.aws_external_id::string as notificationIntegration_aws_external_id,
    payload:aws.notificationIntegration.configuration_complete::string as notificationIntegration_configuration_complete,
    payload:aws.notificationIntegration.iam_role_arn::string as notificationIntegration_iam_role_arn,
    payload:aws.notificationIntegration.iam_user_arn::string as notificationIntegration_iam_user_arn,
    payload:aws.notificationIntegration.policy_arn::string as notificationIntegration_policy_arn,
    payload:aws.notificationIntegration.provisioning_requested::string as notificationIntegration_provisioning_requested,
    payload:aws.notificationIntegration.subscription_arn::string as notificationIntegration_subscription_arn,
    payload:aws.notificationIntegration.subscription_confirmed::string as notificationIntegration_subscription_confirmed,
    payload:aws.notificationIntegration.topic_arn::string as notificationIntegration_topic_arn
from 
    base_parse_json
;


create or replace procedure monitorial_config.deploy_monitorial_sign_up()
returns string
language sql
AS '
BEGIN
CREATE OR REPLACE FUNCTION monitorial_sign_up(company string, email string, givenName string, surname string, region_id string, organisation_name string, account_name string, account_locator string, country string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = ''monitorial_sign_up''
EXTERNAL_ACCESS_INTEGRATIONS = (monitorial_access_integration)
PACKAGES = (''snowflake-snowpark-python'',''requests'')
AS
$$
import _snowflake
import json
import requests

session = requests.Session()

def to_object(company, email, givenName, surname, region_id, organisation_name, account_name, account_locator, country: str):
    data = {}
    data["company"] = company
    data["email"] = email
    data["givenName"] = givenName
    data["surname"] = surname
    data["region_id"] = region_id
    data["organisation_name"] = organisation_name
    data["account_name"] = account_name
    data["account_locator"] = account_locator
    data["country"] = country
    return data

def to_json(data):
    return json.dumps(data)

def monitorial_sign_up(company, email, givenName, surname, region_id, organisation_name, account_name, account_locator, country: str):
    url = ''https://de-monitorial-dev-ae-apim.azure-api.net/admin/monitorial/v1/native_app_sign_up''
    headers = {''Content-type'': ''application/json'',
               ''Ocp-Apim-Subscription-Key'': ''4425d1c68fe74c7880b7ae9744ec5daa'',
               ''Accept'': ''application/json''}

    response = requests.post(url, to_json(to_object(company, email, givenName, surname, region_id, organisation_name, account_name, account_locator, country)), headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return [''Failed to send notification'']
            
   

$$;
END;
';

create or replace procedure monitorial_config.deploy_monitorial_get_all_metadata()
returns string
language sql
AS '
BEGIN
CREATE OR REPLACE FUNCTION monitorial_get_all_metadata()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = ''monitorial_get_rules''
EXTERNAL_ACCESS_INTEGRATIONS = (monitorial_access_integration)
PACKAGES = (''snowflake-snowpark-python'',''requests'')
AS
$$
import _snowflake
import json
import requests

session = requests.Session()

def monitorial_get_rules():
    url = ''https://de-monitorial-dev-ae-apim.azure-api.net/admin/monitorial/v1/configuration''
    headers = {''company_id'': ''3bbdab71-ba27-40d4-85b1-14b3037cecc1'',
               ''Ocp-Apim-Subscription-Key'': ''4425d1c68fe74c7880b7ae9744ec5daa'',
               ''user_id'': ''bbc77e86-81e4-4eff-890c-ea5de4074a68''}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return [''Failed to send notification'']
            
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

create or replace procedure monitorial_config.deploy_network_rule()
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

create or replace procedure monitorial_config.deploy_monitorial_dispatch_func()
returns string
language sql
AS '
BEGIN
CREATE OR REPLACE FUNCTION monitorial_dispatch(object_name string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
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
   GRANT USAGE ON FUNCTION monitorial_sign_up(STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING) to application role monitorial_admin;
   GRANT USAGE ON FUNCTION monitorial_get_all_metadata() to application role monitorial_admin;
   RETURN 'Granted monitorial_dispatch function to monitorial_admin role';
END;
$$;
create or replace procedure monitorial_config.deploy_task_warehouse()
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
grant usage on procedure monitorial_config.deploy_network_rule() to application role monitorial_admin;
grant usage on procedure monitorial_config.deploy_monitorial_dispatch_func() to application role monitorial_admin;
grant usage on procedure monitorial_config.grant_func_privileges() to application role monitorial_admin;
grant usage on procedure monitorial_config.create_task_test() to application role monitorial_admin;
grant usage on procedure monitorial_config.deploy_monitorial_sign_up() to application role monitorial_admin;
grant usage on procedure monitorial_config.deploy_monitorial_get_all_metadata() to application role monitorial_admin;
grant usage on procedure monitorial_config.deploy_setup_monitorial_db_and_assest_creation_sproc() to application role monitorial_admin;
grant usage on procedure monitorial_config.deploy_task_warehouse() to application role monitorial_admin;
grant create task on schema monitorial_config to application role monitorial_admin;
GRANT ALL ON TABLE monitorial_assets.Configs TO APPLICATION ROLE monitorial_admin;
GRANT ALL ON VIEW monitorial_assets.cloud_endpoint TO APPLICATION ROLE monitorial_admin;

--grant usage on warehouse p_task_wh to application role monitorial_admin;
--grant operate on warehouse p_task_wh to application role monitorial_admin;