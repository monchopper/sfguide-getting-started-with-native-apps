-- ==========================================
-- This script runs when the app is installed 
-- ==========================================

-- Create Application Role and Schema
create application role if not exists app_instance_role;
create or alter versioned schema app_instance_schema;

create or replace task app_instance_schema.ENABLED_USER_PREVIOUSLY_DISABLED
	schedule='1440 minute'
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='xsmall'
	as EXECUTE IMMEDIATE $$
            DECLARE
                alert_payload VARIANT;
                alert_message_type VARCHAR DEFAULT '';
                alert_display_message VARCHAR DEFAULT 'User previously disable have been enabled';
                alert_account_name VARCHAR DEFAULT (SELECT current_account());
                alert_name VARCHAR DEFAULT 'PC_MONITORIAL_DB.custom_monitors.enabled_user_previously_disabled';
                alert_severity VARCHAR DEFAULT 'error';
                alert_environment VARCHAR DEFAULT 'unit-test';
                alert_payload_recieved VARCHAR DEFAULT '';
            BEGIN
                
                WITH baseAlertQuery AS (
                       
select
    end_time,
    query_type,
    query_text,
    user_name,
    role_name
from 
    snowflake.account_usage.query_history 
where
    execution_status = 'SUCCESS'
    and query_type = 'ALTER_USER'
    and query_text ilike '%alter user%set disabled = false%'
    and end_time >= dateadd(minutes, -1440, current_timestamp)
order by end_time desc
                ),
                arrayCreation AS (
                        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) AS alert_body
                        FROM baseAlertQuery
                    )
                    SELECT alert_body INTO :alert_payload FROM arrayCreation;
                IF (:alert_payload != []) THEN
                    SELECT pc_monitorial_db.utils.monitorial_dispatch(:alert_account_name,:alert_name,:alert_environment,:alert_message_type,:alert_severity,:alert_display_message,:alert_payload)
                    INTO :alert_payload_recieved;
                    RETURN :alert_payload_recieved;
                ELSE
                    RETURN 'No notification fired';
                END IF;
            END
        $$;

-- Share data
create or replace view app_instance_schema.MFG_SHIPPING as select * from shared_content_schema.MFG_SHIPPING;

-- Create Streamlit app
create or replace streamlit app_instance_schema.streamlit from '/libraries' main_file='streamlit.py';

-- Create UDFs
create or replace function app_instance_schema.cal_lead_time(i int, j int, k int)
returns float
language python
runtime_version = '3.8'
packages = ('snowflake-snowpark-python')
imports = ('/libraries/udf.py')
handler = 'udf.cal_lead_time';

create or replace function app_instance_schema.cal_distance(slat float,slon float,elat float,elon float)
returns float
language python
runtime_version = '3.8'
packages = ('snowflake-snowpark-python','pandas','scikit-learn==1.1.1')
imports = ('/libraries/udf.py')
handler = 'udf.cal_distance';

-- Create Stored Procedure
create or replace procedure app_instance_schema.billing_event(number_of_rows int)
returns string
language python
runtime_version = '3.8'
packages = ('snowflake-snowpark-python')
imports = ('/libraries/procs.py')
handler = 'procs.billing_event';

create or replace function app_instance_schema.hello_world()
returns string
language python
runtime_version = '3.8'
packages = ('snowflake-snowpark-python')
imports = ('/libraries/udf.py')
handler = 'udf.hello_world';



create or replace procedure app_instance_schema.create_network_rule()
returns string
language sql
AS $$
BEGIN
   CREATE OR REPLACE NETWORK RULE app_instance_schema.monitorial_apis_network_rule
   MODE = EGRESS
   TYPE = HOST_PORT
   VALUE_LIST = ('de-monitorial-dev-ae-apim.azure-api.net');
   RETURN 'created network Rule';
END;
$$;

create or replace procedure app_instance_schema.create_network_policy()
returns string
language sql
AS $$
BEGIN
   CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION monitorial_access_integration
   ALLOWED_NETWORK_RULES = (monitorial_apis_network_rule)
   ENABLED = true;
END;
$$;

create or replace procedure app_instance_schema.create_external_function()
returns string
language sql
AS $$
BEGIN
   CREATE OR REPLACE EXTERNAL FUNCTION app_instance_schema.monitorial_dispatch(accountName varchar, objectName varchar, environment varchar, messageType varchar, serverity varchar, message varchar, message_details array)
        RETURNS VARIANT 
        API_INTEGRATION = reference('MONITORIAL_API_INTEGRATION_DEV')
        HEADERS = ('company-id'='3bbdab71-ba27-40d4-85b1-14b3037cecc1', 'subscription-key' = '720109b237994e48bd2aa4b3cf8c403c')
   AS 'https://dbklng06vi.execute-api.ap-southeast-2.amazonaws.com/dev/externalfunction';
 
   grant all on function app_instance_schema.monitorial_dispatch(varchar, varchar, varchar, varchar, varchar, varchar, array) to APPLICATION ROLE app_instance_role;
   RETURN 'created external function : app_instance_schema.monitorial_dispatch';
END;
$$;
CREATE OR REPLACE PROCEDURE  app_instance_schema.call_monitorial_dispatch()
RETURNS  varchar
LANGUAGE SQL
AS
$$
   DECLARE FUNC_SQL VARCHAR;
   BEGIN
      FUNC_SQL := 'SELECT app_instance_schema.monitorial_dispatch(''nativeapp'',''fuckyeahbitch'',''test'',''celebration'',''error'',''fuck me that took ages'',[]);';
      EXECUTE IMMEDIATE :FUNC_SQL;
      RETURN :FUNC_SQL;
   END;
$$;


create or replace procedure app_instance_schema.update_reference(ref_name string, operation string, ref_or_alias string)
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
grant usage on schema app_instance_schema to application role app_instance_role;
grant usage on function app_instance_schema.cal_lead_time(int,int,int) to application role app_instance_role;
grant usage on procedure app_instance_schema.billing_event(int) to application role app_instance_role;
grant usage on function app_instance_schema.cal_distance(float,float,float,float) to application role app_instance_role;
grant SELECT on view app_instance_schema.MFG_SHIPPING to application role app_instance_role;
grant usage on streamlit app_instance_schema.streamlit to application role app_instance_role;
grant usage on procedure app_instance_schema.update_reference(string, string, string) to application role app_instance_role;
grant usage on procedure app_instance_schema.create_external_function() to application role app_instance_role;
grant usage on procedure app_instance_schema.call_monitorial_dispatch() to application role app_instance_role;
grant usage on function app_instance_schema.hello_world() to application role app_instance_role;
