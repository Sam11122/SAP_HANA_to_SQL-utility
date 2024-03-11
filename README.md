# SAP_HANA_to_SQL-utility
This is a python utility which helps to convert graph calculation views in SAP HANA to a series of SQL CTEs. It can take ViewXML from SAP HANA for a particular view and convert each node into an SQL CTE. The code parses the XML and converts it into corresponding SQL queries. It can save countless man-hours of manual migration work where one is migrating from SAP HANA to any new data warehouse.

 

**Features:**

1. The script generates the SQL code step by step, which means the order of the nodes in the calc view is preserved in the generated SQL.
2. It is able to generate SQL for all types of HANA nodes (Aggregation, Projection, Rank, Join, Union)
3. It takes care of all the joining conditions, multiple joins.
4. It converts many SAP HANA functions to Snowflake equivalent ones (like match, in, midstr etc).
 

**Instructions on using the Python utility:**

1. Install Python in your system.
2. Install xmltodict. (pip install xmltodict)
3. Place the python utility (hana_to_sql_utility.py) in some directory. In the same directory, create a folder named xmls.
4. Place the ViewXML(s) of your models inside xmls folder.
5. Run the utility using python hana_to_sql_utility.py or simply run using any IDE.
6. SQL will be generated in the another directory (queries).
