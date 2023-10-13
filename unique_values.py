def valores_unicos(df):

    for col in df.columns:
        
        # Obtém uma lista de valores únicos
        list_of_unique_values = df.select(col).distinct()
    
        # Se o número de valores exclusivos for menor que 15, imprima os valores. 
        # Caso contrário, imprima o número de valores exclusivos
        count_value = list_of_unique_values.count()

        if count_value < 15:
            print("\n")
            print(col + ': ' + str(count_value) + ' valores únicos')
            
            list_unique_values = df.select(col).distinct().rdd.flatMap(lambda x: x).collect()
            print(str(list_unique_values))

        else:
            print("\n")
            print(col + ': ' + str(count_value) + ' valores únicos')
