-- ==========================================
-- This script runs when the app is installed 
-- ==========================================

-- Create Application Role and Schema
create application role if not exists app_instance_role;
create or alter versioned schema app_instance_schema;
CREATE OR ALTER VERSIONED SCHEMA app_code;
CREATE STAGE app_code.app_jars;



-- Create Streamlit app
create or replace streamlit app_instance_schema.streamlit from '/libraries' main_file='streamlit.py';
create or replace procedure app_instance_schema.create_network_rule()
returns string
language sql
AS $$
BEGIN
   CREATE OR REPLACE NETWORK RULE external_database_network_rule
      TYPE = HOST_PORT
      VALUE_LIST = ('omnata-sandpit.database.windows.net:1433') 
      MODE= EGRESS;
   grant usage on network rule external_database_network_rule to application role app_instance_role;
   RETURN 'created network Rule';
END;
$$;

create or replace procedure app_instance_schema.create_network_secret()
returns string
language sql
AS $$
BEGIN
   CREATE OR REPLACE SECRET external_database_cred
      TYPE = password
      USERNAME = 'administrator-omnata'
      PASSWORD = 'M0nch0pp3r^000';
   grant usage on secret external_database_cred to application role app_instance_role;
   RETURN 'created network Secret';
END;
$$;

create or replace procedure app_instance_schema.create_network_policy()
returns string
language sql
AS $$
BEGIN
   CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION external_database_network_rule_ext_int
      ALLOWED_NETWORK_RULES = (external_database_network_rule)
      ALLOWED_AUTHENTICATION_SECRETS = (external_database_cred)
      ENABLED = true;
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
grant usage on schema app_code to application role app_instance_role;
grant usage on streamlit app_instance_schema.streamlit to application role app_instance_role;
grant usage on procedure app_instance_schema.create_network_rule() to application role app_instance_role;
grant usage on procedure app_instance_schema.create_network_secret() to application role app_instance_role;
--grant usage on procedure app_instance_schema.create_read_jdbc() to application role app_instance_role;
--grant usage on secret app_instance_schema.external_database_cred to application role app_instance_role;
--grant usage on network rule app_instance_schema.external_database_network_rule to application role app_instance_role;