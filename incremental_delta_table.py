def incremental_delta_table(df, table: str, table_temp: str, merge_condition_fields: list):
    try:
        table_temp = f"{table_temp}_temp_view"
        df.createOrReplaceTempView(table_temp)
        columns = df.columns

        condition = ' AND '.join(list(map(lambda field: f"{table}.{field} = {table_temp}.{field}", merge_condition_fields)))

        update_fields = ',\n'.join(list(map(lambda field: f"{table}.{field} = {table_temp}.{field}" if field != "DH_PROCESSAMENTO" else f"{table}.{field} = CURRENT_TIMESTAMP()", columns)))
        insert_columns = ',\n'.join(list(map(lambda field: f"{field}", columns)))
        insert_fields = ',\n'.join(list(map(lambda field: f"{table_temp}.{field}" if field != "DH_PROCESSAMENTO" else f"{field} = CURRENT_TIMESTAMP()", columns)))

        query = f"""
            MERGE INTO {table} 
            USING {table_temp} 
            ON {condition}
            WHEN MATCHED THEN UPDATE SET {update_fields}
            WHEN NOT MATCHED THEN INSERT ({insert_columns}) 
            VALUES ({insert_fields});
        """

        spark.sql(query).display()

    except Exception as err:
        raise err
