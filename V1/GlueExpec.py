import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from great_expectations.dataset import SparkDFDataset

# Inicialize o GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Carregue os dados dos arquivos CSV
df = spark.read.csv('s3://my-bucket/raw/us-counties-2020.csv', header=True)

# Converte o Spark DataFrame para um GE DataFrame
ge_df = SparkDFDataset(df)

# Cria um novo conjunto de Expectations
expectation_suite = ge_df.create_expectation_suite()

# Adicione suas Expectations
ge_df.expect_column_values_to_not_be_null("date")
ge_df.expect_column_values_to_not_be_null("county")
ge_df.expect_column_values_to_not_be_null("state")
ge_df.expect_column_values_to_not_be_null("cases")
ge_df.expect_column_values_to_not_be_null("deaths")
ge_df.expect_column_values_to_be_of_type("date", "DateType")
ge_df.expect_column_values_to_be_of_type("cases", "IntegerType")
ge_df.expect_column_values_to_be_of_type("deaths", "IntegerType")
ge_df.expect_column_values_to_be_between("cases", 0, None)
ge_df.expect_column_values_to_be_between("deaths", 0, None)

# Salve o conjunto de Expectations
ge_df.save_expectation_suite('s3://my-bucket/expectations/us-counties-2020.json')

# Salve os dados brutos na camada Raw do seu Data Lake
df.write.csv('s3://my-bucket/raw/us-counties-2020.csv')