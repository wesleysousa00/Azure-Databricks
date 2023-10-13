databases = sqlContext.sql("SHOW DATABASES").collect()

for db in databases:
    db_name = db.databaseName if db.databaseName != "default" else None

    if db_name: 
        tables = sqlContext.sql(f"SHOW TABLES IN {db_name}").select(["database", "tableName"]).collect()
        
        for row in tables:
            delta_table_name = f"{row.database}.{row.tableName}"

            # SET RETENTION LOG PERIOD
            spark.sql(f"""
                      CREATE OR REPLACE TABLE {delta_table_name} SHALLOW CLONE {delta_table_name}
                      TBLPROPERTIES (
                          delta.logRetentionDuration = '7 days',
                          delta.deletedFileRetentionDuration = '7 days'
                        )          
            """)
