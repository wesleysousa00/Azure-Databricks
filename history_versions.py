import pytz
def history_versions(database, table) -> str:
    saopaulo_timezone = pytz.timezone("America/Sao_Paulo")
    listVersions = spark.sql(f"describe history {database}.{table}").collect()
 
    for row in listVersions:
        print(f'Version -> {row.version}  Count ->  {spark.sql(f"select count(*) as qtd from {database}.{table} VERSION AS OF {row.version}").collect()[0][0]} Date -> {row.timestamp.astimezone(saopaulo_timezone)}')
        
