# Vacuum routine for cleaning files > 7 days
num_hours = 168 

from delta.tables import *

databases = sqlContext.sql("SHOW DATABASES").collect()

for db in databases:
    db_name = db.databaseName if db.databaseName != "default" else None

    if db_name: 
        tables = sqlContext.sql(f"SHOW TABLES IN {db_name}").select(["database", "tableName"]).collect()
        
        for row in tables:
            delta_table_name = f"{row.database}.{row.tableName}"

            delta_table = DeltaTable.forName(spark, delta_table_name)
            delta_table.vacuum(num_hours)
