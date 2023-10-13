def cont_duplicados(df):
    contagem = df.count()
    registros_duplicados = contagem - df.dropDuplicates().count()
    print(f'O Dataframe possui {contagem} registros \n\
O Dataframe possui {registros_duplicados} registros duplicados')
