import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Raw - Facilities - Fix
RawFacilitiesFix_node1687261092337 = glueContext.create_dynamic_frame.from_catalog(
    database="covid-raw",
    table_name="nytimes-raw-facilities",
    transformation_ctx="RawFacilitiesFix_node1687261092337",
)

# Script generated for node Change Schema - Facilities
ChangeSchemaFacilities_node1687261148207 = ApplyMapping.apply(
    frame=RawFacilitiesFix_node1687261092337,
    mappings=[
        ("nyt_id", "string", "nyt_id", "string"),
        ("facility_name", "string", "facility_name", "string"),
        ("facility_type", "string", "facility_type", "string"),
        ("facility_city", "string", "facility_city", "string"),
        ("facility_county", "string", "facility_county", "string"),
        ("facility_county_fips", "long", "facility_county_fips", "bigint"),
        ("facility_state", "string", "facility_state", "string"),
        ("facility_lng", "double", "facility_lng", "double"),
        ("facility_lat", "double", "facility_lat", "double"),
        ("latest_inmate_population", "long", "latest_inmate_population", "long"),
        ("max_inmate_population_2020", "long", "max_inmate_population_2020", "long"),
        ("total_inmate_cases", "long", "total_inmate_cases", "long"),
        ("total_inmate_deaths", "long", "total_inmate_deaths", "long"),
        ("total_officer_cases", "long", "total_officer_cases", "long"),
        ("total_officer_deaths", "long", "total_officer_deaths", "long"),
        ("note", "string", "note", "string"),
    ],
    transformation_ctx="ChangeSchemaFacilities_node1687261148207",
)

# Script generated for node Trusted - facilities
Trustedfacilities_node1687261421176 = glueContext.getSink(
    path="s3://usp-prjint-covid/trusted/facilities/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="Trustedfacilities_node1687261421176",
)
Trustedfacilities_node1687261421176.setCatalogInfo(
    catalogDatabase="covid-trusted", catalogTableName="nytimes-trusted-facilities"
)
Trustedfacilities_node1687261421176.setFormat("glueparquet")
Trustedfacilities_node1687261421176.writeFrame(ChangeSchemaFacilities_node1687261148207)
job.commit()
