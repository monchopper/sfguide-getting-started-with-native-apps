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
   grant read on secret external_database_cred to application role app_instance_role;
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

create or replace procedure app_instance_schema.create_read_jdbc()
returns string
language sql
AS '
BEGIN
  CREATE OR REPLACE FUNCTION READ_JDBC_NATIVE(OPTION OBJECT, query STRING) 
  RETURNS TABLE(data OBJECT)
  LANGUAGE JAVA
  RUNTIME_VERSION = ''11''
  IMPORTS = (''/libraries/mssql-jdbc-12.4.2.jre11.jar'')
  EXTERNAL_ACCESS_INTEGRATIONS = (monitorial_access_integration)
  SECRETS = (''cred'' = SQL_SYNC_SANDPIT.APP_INSTANCE_SCHEMA.external_database_cred )
  HANDLER = ''JdbcDataReader''
AS $$
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;
import com.snowflake.snowpark_java.types.SnowflakeSecrets;

public class JdbcDataReader {

    public static class OutputRow {
        public Map<String, String> data;

        public OutputRow(Map<String, String> data) {
            this.data = data;
        }
    }

    public static Class getOutputClass() {
      return OutputRow.class;
    }

    public Stream<OutputRow> process(Map<String, String> jdbcConfig, String query) {
        String jdbcUrl = jdbcConfig.get("url");
        String username;
        String password;
        
        if ("true".equals(jdbcConfig.get("use_secrets")))
        {
            SnowflakeSecrets sfSecrets = SnowflakeSecrets.newInstance();
            var secret = sfSecrets.getUsernamePassword("cred");
            username   = secret.getUsername();
            password   = secret.getPassword();
        }
        else 
        {
            username = jdbcConfig.get("username");
            password = jdbcConfig.get("password");
        }
        try {
            // Load the JDBC driver 
            Class.forName(jdbcConfig.get("driver"));
            // Create a connection to the database
            Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
            // Create a statement for executing SQL queries
            Statement statement = connection.createStatement();
            // Execute the query
            ResultSet resultSet = statement.executeQuery(query);
            // Get metadata about the result set
            ResultSetMetaData metaData = resultSet.getMetaData();
            // Create a list of column names
            List<String> columnNames = new ArrayList<>();
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                columnNames.add(metaData.getColumnName(i));
            }
            // Convert the ResultSet to a Stream of OutputRow objects
            Stream<OutputRow> resultStream = Stream.generate(() -> {
                try {
                    if (resultSet.next()) {
                        Map<String, String> rowMap = new HashMap<>();
                        for (String columnName : columnNames) {
                            String columnValue = resultSet.getString(columnName);
                            rowMap.put(columnName, columnValue);
                        }
                        return new OutputRow(rowMap);
                    } else {
                        // Close resources
                        resultSet.close();
                        statement.close();
                        connection.close();                        
                        return null;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }
            }).takeWhile(Objects::nonNull);
            return resultStream;
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, String> rowMap = new HashMap<>();
            rowMap.put("ERROR",e.toString());
            return Stream.of(new OutputRow(rowMap));
        }
    }
}
$$;
grant usage on function READ_JDBC_NATIVE(OBJECT, STRING)  to application role app_instance_role;
END;
';

-- Grant usage and permissions on objects
grant usage on schema app_instance_schema to application role app_instance_role;
grant usage on schema app_code to application role app_instance_role;
grant usage on streamlit app_instance_schema.streamlit to application role app_instance_role;
grant usage on procedure app_instance_schema.create_network_rule() to application role app_instance_role;
grant usage on procedure app_instance_schema.create_network_secret() to application role app_instance_role;
--grant usage on procedure app_instance_schema.create_read_jdbc() to application role app_instance_role;
--grant usage on secret app_instance_schema.external_database_cred to application role app_instance_role;
--grant usage on network rule app_instance_schema.external_database_network_rule to application role app_instance_role;