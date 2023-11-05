-- ==========================================
-- This script runs when the app is installed 
-- ==========================================

-- Create Application Role and Schema
create application role if not exists monitorial_admin;
create or alter versioned schema monitorial_config;

-- Create Streamlit app
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
create or replace procedure monitorial_config.grant_monitorial_dispatch_func()
returns string
language sql
AS $$
BEGIN
   GRANT USAGE ON FUNCTION monitorial_dispatch(STRING) to application role monitorial_admin;
   RETURN 'Granted monitorial_dispatch function to monitorial_admin role';
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
grant create secret on schema monitorial_config to application role monitorial_admin;
grant create network rule on schema monitorial_config to application role monitorial_admin;
grant usage on streamlit monitorial_config.streamlit to application role monitorial_admin;
grant usage on procedure monitorial_config.create_network_rule() to application role monitorial_admin;
grant usage on procedure monitorial_config.create_monitorial_dispatch_func() to application role monitorial_admin;
grant usage on procedure monitorial_config.grant_monitorial_dispatch_func() to application role monitorial_admin;