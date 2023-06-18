import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Função para executar as transformações em cada tabela
def process_table(source_table, target_path, target_table):
    # Leitura dos dados da tabela de origem
    source_data = glueContext.create_dynamic_frame.from_catalog(
        database="covid-raw",
        table_name=source_table,
        transformation_ctx=f"{source_table}_source"
    )

    # Transformações nos dados
    # Adicione aqui as transformações específicas para cada tabela
    
    transformed_data = source_data

    # Criação da coluna de partição
    transformed_data = transformed_data.withColumn("date_partition", transformed_data["date"].cast("string"))

    # Caminho de saída para os dados particionados
    output_path = f"s3://usp-prjint-covid/trusted/nytimes/{target_path}/"

    # Escrita dos dados particionados em formato Parquet
    glueContext.write_dynamic_frame.from_options(
        frame=transformed_data,
        connection_type="s3",
        connection_options={"path": output_path},
        format="parquet",
        compression="snappy"
        transformation_ctx=f"{source_table}_sink"
    )

    # Atualização do catálogo do Glue com a tabela particionada
    glueContext.update_catalog_table(
        database="covid-trusted",
        table_name=target_table,
        new_table_path=output_path,
        new_table_partition_cols=["date_partition"]
    )

# Inicialização do contexto do Glue
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Execução das transformações para cada tabela
process_table("nytimes-raw-us_states", "us-states", "nytimes-trusted-us_states-pqt")
process_table("nytimes-raw-anomalies", "anomalies", "nytimes-trusted-anomalies-pqt")
process_table("nytimes-raw-colleges", "colleges", "nytimes-trusted-colleges-pqt")
process_table("nytimes-raw-deaths", "deaths", "nytimes-trusted-deaths-pqt")
process_table("nytimes-raw-facilities", "facilities", "nytimes-trusted-facilities-pqt")
process_table("nytimes-raw-mask_use_by_county", "mask-use-by-county", "nytimes-trusted-mask_use_by_county-pqt")
process_table("nytimes-raw-systems", "systems", "nytimes-trusted-systems-pqt")
process_table("nytimes-raw-us_consl", "us-consl", "nytimes-trusted-us_consl-pqt")
process_table("nytimes-raw-us_counties", "us-counties", "nytimes-trusted-us_counties-pqt")
process_table("nytimes-raw-us_counties_consl", "us-counties-consl", "nytimes-trusted-us_counties_consl-pqt")

# Fim do job
job.commit()