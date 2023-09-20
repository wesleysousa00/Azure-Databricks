"""
Create a table if it doesn't exist, then copy data from an external source into it.
The source is specified as a query to an Azure Data Lake Storage location using Parquet format.
Schema inference is enabled, and schema merging is allowed if necessary.
"""


CREATE TABLE IF NOT EXISTS <schema_name>.<table_name>;


COPY INTO <schema_name>.<table_name>
FROM 
(SELECT *, _metadata 
FROM 'abfss://<container_name>@<storageaccount>.dfs.core.windows.net/path/*')

FILEFORMAT = PARQUET
FORMAT_OPTIONS ('inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');