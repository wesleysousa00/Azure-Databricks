def capture_delta_table_changes(source_table: str):

    describe_history = spark.sql(f"DESCRIBE HISTORY {source_table};")
    max_table_version = describe_history.select(f.max("version").alias("max_version"))
    max_table_version = max_table_version.head()[0]

    changes_df = spark.read.format("delta")\
                           .option("readChangeData", "true")\
                           .option("startingVersion", max_table_version)\
                           .table(source_table)

    changes_df = changes_df.where("_change_type != 'update_preimage' ")
    table_fields = [col for col in changes_df.columns if col not in ('_commit_version', '_commit_timestamp')]

    df_updated = changes_df.select(table_fields)
    return df_updated
