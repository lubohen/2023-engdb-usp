# Carga dos dados confiáveis da camada Trusted
df = spark.read.csv('s3://my-bucket/trusted/us-counties-2020.csv', header=True)

# Conversao do Spark DataFrame para um GE DataFrame
ge_df = SparkDFDataset(df)

# Carga do conjunto de Expectations
expectation_suite = ge_df.load_expectation_suite('s3://my-bucket/expectations/us-counties-2020.json')

# Processamento de dados 

# Validacao  dados
validation_result = ge_df.validate(expectation_suite=expectation_suite)

# Verifique se a validação foi bem-sucedida
if validation_result["success"]:
    print("Os dados atendem às Expectations.")
else:
    print("Os dados não atendem às Expectations.")


# Salve os dados processados na camada Specialized do seu Data Lake
df.write.csv('s3://my-bucket/specialized/us-counties-2020.csv')
