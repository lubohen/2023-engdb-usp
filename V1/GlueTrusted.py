# Carregue os dados brutos da camada Raw
df = spark.read.csv('s3://my-bucket/raw/us-counties-2020.csv', header=True)

# Converte o Spark DataFrame para um GE DataFrame
ge_df = SparkDFDataset(df)

# Carregue o conjunto de Expectations
expectation_suite = ge_df.load_expectation_suite('s3://my-bucket/expectations/us-counties-2020.json')

# Processamento de dados (remova dados duplicados, dados corrompidos, padronize a tipagem e o formato dos dados, verifique a completude e precisão)
# O código para isso foi detalhado na resposta anterior

# Valide seus dados
validation_result = ge_df.validate(expectation_suite=expectation_suite)

# Verifique se a validação foi bem-sucedida

if validation_result["success"]:
    print("Os dados atendem às Expectations.")
else:
    print("Os dados não atendem às Expectations.")

# Salve os dados processados na camada Trusted do seu Data Lake
df.write.csv('s3://my-bucket/trusted/us-counties-2020.csv')
