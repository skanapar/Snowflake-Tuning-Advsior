This is a Snowflake Streamlit APP to suggest  Tuning recommendations for a given Query ID.

The Input parameters are Query-Hash.


Functionality:
1. Checks if teh quey is valid Query
2. Displays the Actual SQL
3. Checks for the bottlenecks based on actual execution
4. Provides GenAI based recommendations to fix the query
5. Optionally executes the query
6. If Query needs to be executed without caching an option is provided in the UI


Permissions reqd:

Cortex_user
GRANT DATABASE ROLE SNOWFLAKE.GOVERNANCE_VIEWER TO ROLE <custom_role_name>;
Actual role to execute a query


