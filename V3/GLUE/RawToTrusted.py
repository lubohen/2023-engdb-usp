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

# Script generated for node raw-us_conties
rawus_conties_node1687029475026 = glueContext.create_dynamic_frame.from_catalog(
    database="covid-raw",
    table_name="nytimes-raw-us_counties",
    transformation_ctx="rawus_conties_node1687029475026",
)

# Script generated for node raw-us_counties_consl
rawus_counties_consl_node1687029475642 = glueContext.create_dynamic_frame.from_catalog(
    database="covid-raw",
    table_name="nytimes-raw-us_counties_consl",
    transformation_ctx="rawus_counties_consl_node1687029475642",
)

# Script generated for node raw-mask
rawmask_node1687029473282 = glueContext.create_dynamic_frame.from_catalog(
    database="covid-raw",
    table_name="nytimes-raw-mask_use_by_county",
    transformation_ctx="rawmask_node1687029473282",
)

# Script generated for node raw-facilities
rawfacilities_node1687029472698 = glueContext.create_dynamic_frame.from_catalog(
    database="covid-raw",
    table_name="nytimes-raw-facilities",
    transformation_ctx="rawfacilities_node1687029472698",
)

# Script generated for node raw-us_counties_recents
rawus_counties_recents_node1687029476274 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="covid-raw",
        table_name="nytimes-raw-us_counties_recents",
        transformation_ctx="rawus_counties_recents_node1687029476274",
    )
)

# Script generated for node raw-anomalies
rawanomalies_node1687028732568 = glueContext.create_dynamic_frame.from_catalog(
    database="covid-raw",
    table_name="nytimes-raw-anomalies",
    transformation_ctx="rawanomalies_node1687028732568",
)

# Script generated for node raw-us_consl
rawus_consl_node1687029474441 = glueContext.create_dynamic_frame.from_catalog(
    database="covid-raw",
    table_name="nytimes-raw-us_consl",
    transformation_ctx="rawus_consl_node1687029474441",
)

# Script generated for node raw-colleges
rawcolleges_node1687029470682 = glueContext.create_dynamic_frame.from_catalog(
    database="covid-raw",
    table_name="nytimes-raw-colleges",
    transformation_ctx="rawcolleges_node1687029470682",
)

# Script generated for node raw-us_states
rawus_states_node1687029476882 = glueContext.create_dynamic_frame.from_catalog(
    database="covid-raw",
    table_name="nytimes-raw-us_states",
    transformation_ctx="rawus_states_node1687029476882",
)

# Script generated for node raw-systems
rawsystems_node1687029473858 = glueContext.create_dynamic_frame.from_catalog(
    database="covid-raw",
    table_name="nytimes-raw-systems",
    transformation_ctx="rawsystems_node1687029473858",
)

# Script generated for node raw-deaths
rawdeaths_node1687029472009 = glueContext.create_dynamic_frame.from_catalog(
    database="covid-raw",
    table_name="nytimes-raw-deaths",
    transformation_ctx="rawdeaths_node1687029472009",
)

# Script generated for node Change Schema-us_conties
ChangeSchemaus_conties_node1687031700348 = ApplyMapping.apply(
    frame=rawus_conties_node1687029475026,
    mappings=[
        ("date", "string", "date", "date"),
        ("county", "string", "county", "string"),
        ("state", "string", "state", "string"),
        ("fips", "long", "fips", "bigint"),
        ("cases", "long", "cases", "long"),
        ("deaths", "long", "deaths", "long"),
    ],
    transformation_ctx="ChangeSchemaus_conties_node1687031700348",
)

# Script generated for node Change Schema-counties_consl
ChangeSchemacounties_consl_node1687031854454 = ApplyMapping.apply(
    frame=rawus_counties_consl_node1687029475642,
    mappings=[
        ("date", "string", "date", "date"),
        ("county", "string", "county", "string"),
        ("state", "string", "state", "string"),
        ("fips", "long", "fips", "bigint"),
        ("cases", "long", "cases", "long"),
        ("deaths", "long", "deaths", "long"),
    ],
    transformation_ctx="ChangeSchemacounties_consl_node1687031854454",
)

# Script generated for node Change Schema - mask
ChangeSchemamask_node1687031103585 = ApplyMapping.apply(
    frame=rawmask_node1687029473282,
    mappings=[
        ("countyfp", "long", "countyfp", "bigint"),
        ("never", "double", "never", "double"),
        ("rarely", "double", "rarely", "double"),
        ("sometimes", "double", "sometimes", "double"),
        ("frequently", "double", "frequently", "double"),
        ("always", "double", "always", "double"),
    ],
    transformation_ctx="ChangeSchemamask_node1687031103585",
)

# Script generated for node Change Schema-facilities
ChangeSchemafacilities_node1687030782354 = ApplyMapping.apply(
    frame=rawfacilities_node1687029472698,
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
    transformation_ctx="ChangeSchemafacilities_node1687030782354",
)

# Script generated for node Change Schema-us_counties_recents
ChangeSchemaus_counties_recents_node1687031978514 = ApplyMapping.apply(
    frame=rawus_counties_recents_node1687029476274,
    mappings=[
        ("date", "string", "date", "date"),
        ("county", "string", "county", "string"),
        ("state", "string", "state", "string"),
        ("fips", "long", "fips", "bigint"),
        ("cases", "long", "cases", "long"),
        ("deaths", "long", "deaths", "long"),
    ],
    transformation_ctx="ChangeSchemaus_counties_recents_node1687031978514",
)

# Script generated for node Change Schema - Anomalies
ChangeSchemaAnomalies_node1687029893353 = ApplyMapping.apply(
    frame=rawanomalies_node1687028732568,
    mappings=[
        ("date", "string", "date", "date"),
        ("end_date", "string", "end_date", "date"),
        ("county", "string", "county", "string"),
        ("state", "string", "state", "string"),
        ("geoid", "string", "geoid", "string"),
        ("type", "string", "type", "string"),
        ("omit_from_rolling_average", "string", "omit_from_rolling_average", "string"),
        (
            "omit_from_rolling_average_on_subgeographies",
            "string",
            "omit_from_rolling_average_on_subgeographies",
            "string",
        ),
        (
            "adjusted_daily_count_for_avg",
            "long",
            "adjusted_daily_count_for_avg",
            "long",
        ),
        ("description", "string", "description", "string"),
    ],
    transformation_ctx="ChangeSchemaAnomalies_node1687029893353",
)

# Script generated for node Change Schema-us_consl
ChangeSchemaus_consl_node1687031558939 = ApplyMapping.apply(
    frame=rawus_consl_node1687029474441,
    mappings=[
        ("date", "string", "date", "date"),
        ("cases", "long", "cases", "long"),
        ("deaths", "long", "deaths", "long"),
    ],
    transformation_ctx="ChangeSchemaus_consl_node1687031558939",
)

# Script generated for node Change Schema - colleges
ChangeSchemacolleges_node1687030229683 = ApplyMapping.apply(
    frame=rawcolleges_node1687029470682,
    mappings=[
        ("date", "string", "date", "date"),
        ("state", "string", "state", "string"),
        ("county", "string", "county", "string"),
        ("city", "string", "city", "string"),
        ("ipeds_id", "string", "ipeds_id", "bigint"),
        ("college", "string", "college", "string"),
        ("cases", "long", "cases", "long"),
        ("cases_2021", "long", "cases_2021", "long"),
        ("notes", "string", "notes", "string"),
    ],
    transformation_ctx="ChangeSchemacolleges_node1687030229683",
)

# Script generated for node Change Schema-us_states
ChangeSchemaus_states_node1687032220360 = ApplyMapping.apply(
    frame=rawus_states_node1687029476882,
    mappings=[
        ("date", "string", "date", "date"),
        ("state", "string", "state", "string"),
        ("fips", "long", "fips", "bigint"),
        ("cases", "long", "cases", "long"),
        ("deaths", "long", "deaths", "long"),
    ],
    transformation_ctx="ChangeSchemaus_states_node1687032220360",
)

# Script generated for node Change Schema-systems
ChangeSchemasystems_node1687031393322 = ApplyMapping.apply(
    frame=rawsystems_node1687029473858,
    mappings=[
        ("system", "string", "system", "string"),
        ("inmate_tests", "long", "inmate_tests", "long"),
        ("total_inmate_cases", "long", "total_inmate_cases", "long"),
        ("total_inmate_deaths", "long", "total_inmate_deaths", "long"),
        ("latest_inmate_population", "long", "latest_inmate_population", "long"),
        ("max_inmate_population_2020", "long", "max_inmate_population_2020", "long"),
        ("total_officer_cases", "long", "total_officer_cases", "long"),
        ("total_officer_deaths", "long", "total_officer_deaths", "long"),
    ],
    transformation_ctx="ChangeSchemasystems_node1687031393322",
)

# Script generated for node Change Schema - deaths
ChangeSchemadeaths_node1687030434731 = ApplyMapping.apply(
    frame=rawdeaths_node1687029472009,
    mappings=[
        ("country", "string", "country", "string"),
        ("placename", "string", "placename", "string"),
        ("frequency", "string", "frequency", "string"),
        ("start_date", "string", "start_date", "date"),
        ("end_date", "string", "end_date", "date"),
        ("year", "string", "year", "bigint"),
        ("month", "long", "month", "bigint"),
        ("week", "long", "week", "bigint"),
        ("deaths", "long", "deaths", "long"),
        ("expected_deaths", "long", "expected_deaths", "long"),
        ("excess_deaths", "long", "excess_deaths", "long"),
        ("baseline", "string", "baseline", "string"),
    ],
    transformation_ctx="ChangeSchemadeaths_node1687030434731",
)

# Script generated for node trusted-counties
trustedcounties_node1687031732641 = glueContext.getSink(
    path="s3://usp-prjint-covid/trusted/us_counties/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["date"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="trustedcounties_node1687031732641",
)
trustedcounties_node1687031732641.setCatalogInfo(
    catalogDatabase="covid-trusted", catalogTableName="nytimes-trusted-counties"
)
trustedcounties_node1687031732641.setFormat("glueparquet")
trustedcounties_node1687031732641.writeFrame(ChangeSchemaus_conties_node1687031700348)
# Script generated for node trusted-counties_consl
trustedcounties_consl_node1687031903390 = glueContext.getSink(
    path="s3://usp-prjint-covid/trusted/counties_consl/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["date"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="trustedcounties_consl_node1687031903390",
)
trustedcounties_consl_node1687031903390.setCatalogInfo(
    catalogDatabase="covid-trusted", catalogTableName="nytimes-trusted-counties_consl"
)
trustedcounties_consl_node1687031903390.setFormat("glueparquet")
trustedcounties_consl_node1687031903390.writeFrame(
    ChangeSchemacounties_consl_node1687031854454
)
# Script generated for node trusted-mask
trustedmask_node1687031208182 = glueContext.getSink(
    path="s3://usp-prjint-covid/trusted/mask/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="trustedmask_node1687031208182",
)
trustedmask_node1687031208182.setCatalogInfo(
    catalogDatabase="covid-trusted", catalogTableName="nytimes-trusted-mask"
)
trustedmask_node1687031208182.setFormat("glueparquet")
trustedmask_node1687031208182.writeFrame(ChangeSchemamask_node1687031103585)
# Script generated for node trusted-facilities
trustedfacilities_node1687030984636 = glueContext.getSink(
    path="s3://usp-prjint-covid/trusted/facilities/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="trustedfacilities_node1687030984636",
)
trustedfacilities_node1687030984636.setCatalogInfo(
    catalogDatabase="covid-trusted", catalogTableName="nytimes-trusted-facilities"
)
trustedfacilities_node1687030984636.setFormat("glueparquet")
trustedfacilities_node1687030984636.writeFrame(ChangeSchemafacilities_node1687030782354)
# Script generated for node trusted-us_counties_recents
trustedus_counties_recents_node1687032082407 = glueContext.getSink(
    path="s3://usp-prjint-covid/trusted/us_counties_recents/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["date"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="trustedus_counties_recents_node1687032082407",
)
trustedus_counties_recents_node1687032082407.setCatalogInfo(
    catalogDatabase="covid-trusted",
    catalogTableName="nytimes-trusted-us_counties_recents",
)
trustedus_counties_recents_node1687032082407.setFormat("glueparquet")
trustedus_counties_recents_node1687032082407.writeFrame(
    ChangeSchemaus_counties_recents_node1687031978514
)
# Script generated for node trusted - Anomalies
trustedAnomalies_node1687030056394 = glueContext.getSink(
    path="s3://usp-prjint-covid/trusted/anomalies/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["date"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="trustedAnomalies_node1687030056394",
)
trustedAnomalies_node1687030056394.setCatalogInfo(
    catalogDatabase="covid-trusted", catalogTableName="nytimes-trusted-anomalies"
)
trustedAnomalies_node1687030056394.setFormat("glueparquet")
trustedAnomalies_node1687030056394.writeFrame(ChangeSchemaAnomalies_node1687029893353)
# Script generated for node trusted-us_consl
trustedus_consl_node1687031599390 = glueContext.getSink(
    path="s3://usp-prjint-covid/trusted/us_consl/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["date"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="trustedus_consl_node1687031599390",
)
trustedus_consl_node1687031599390.setCatalogInfo(
    catalogDatabase="covid-trusted", catalogTableName="nytimes-trusted-us_consl"
)
trustedus_consl_node1687031599390.setFormat("glueparquet")
trustedus_consl_node1687031599390.writeFrame(ChangeSchemaus_consl_node1687031558939)
# Script generated for node trusted-colleges
trustedcolleges_node1687030327256 = glueContext.getSink(
    path="s3://usp-prjint-covid/trusted/colleges/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["date"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="trustedcolleges_node1687030327256",
)
trustedcolleges_node1687030327256.setCatalogInfo(
    catalogDatabase="covid-trusted", catalogTableName="nytimes-trusted-colleges"
)
trustedcolleges_node1687030327256.setFormat("glueparquet")
trustedcolleges_node1687030327256.writeFrame(ChangeSchemacolleges_node1687030229683)
# Script generated for node trusted-us_states
trustedus_states_node1687032257681 = glueContext.getSink(
    path="s3://usp-prjint-covid/trusted/us_states/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["date"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="trustedus_states_node1687032257681",
)
trustedus_states_node1687032257681.setCatalogInfo(
    catalogDatabase="covid-trusted", catalogTableName="nytimes-trusted-us_states"
)
trustedus_states_node1687032257681.setFormat("glueparquet")
trustedus_states_node1687032257681.writeFrame(ChangeSchemaus_states_node1687032220360)
# Script generated for node trusted-systems
trustedsystems_node1687031427393 = glueContext.getSink(
    path="s3://usp-prjint-covid/trusted/systems/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="trustedsystems_node1687031427393",
)
trustedsystems_node1687031427393.setCatalogInfo(
    catalogDatabase="covid-trusted", catalogTableName="nytimes-trusted-systems"
)
trustedsystems_node1687031427393.setFormat("glueparquet")
trustedsystems_node1687031427393.writeFrame(ChangeSchemasystems_node1687031393322)
# Script generated for node trusted-deaths
trusteddeaths_node1687030607609 = glueContext.getSink(
    path="s3://usp-prjint-covid/trusted/deaths/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["year", "month"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="trusteddeaths_node1687030607609",
)
trusteddeaths_node1687030607609.setCatalogInfo(
    catalogDatabase="covid-trusted", catalogTableName="nytimes-trusted-deaths"
)
trusteddeaths_node1687030607609.setFormat("glueparquet")
trusteddeaths_node1687030607609.writeFrame(ChangeSchemadeaths_node1687030434731)
job.commit()
