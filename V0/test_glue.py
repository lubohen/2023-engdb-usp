import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Inicializa o contexto do Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)

# Define os caminhos do bucket S3 de origem e destino
s3_input_path = "s3://meu-bucket-origem/prefixo/"
s3_output_path = "s3://meu-bucket-destino/prefixo/"

# Lê os dados CSV do bucket S3 de origem
source_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s3_input_path)

# Escreve os dados CSV no bucket S3 de destino
source_df.write.format("csv").option("header", "true").mode("overwrite").save(s3_output_path)

# Finaliza o job
job.commit()
