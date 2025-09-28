import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from pyspark.context import SparkContext

# Job initialization
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

# Load curated customer dataset
customers_curated = glue_ctx.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://vs-stedi-human-balance-analytics/customer/curated/"],
        "recurse": True,
    },
    format_options={"multiline": False},
)

# Load raw step trainer dataset (landing zone)
step_trainer_landing = glue_ctx.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://vs-stedi-human-balance-analytics/step_trainer/landing/"],
        "recurse": True,
    },
    format_options={"multiline": False},
)

# Prepare customer dataset for join (rename fields to avoid collision)
customers_mapped = ApplyMapping.apply(
    frame=customers_curated,
    mappings=[
        ("serialNumber", "string", "cust_serialNumber", "string"),
        ("birthDay", "string", "birthDay", "string"),
        ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "long"),
        ("registrationDate", "bigint", "registrationDate", "long"),
        ("customerName", "string", "customerName", "string"),
        ("email", "string", "email", "string"),
        ("lastUpdateDate", "bigint", "lastUpdateDate", "long"),
        ("phone", "string", "phone", "string"),
        ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "long"),
        ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "long"),
    ],
)

# Join step trainer landing with curated customer info
joined_data = Join.apply(
    frame1=step_trainer_landing,
    frame2=customers_mapped,
    keys1=["serialNumber"],
    keys2=["cust_serialNumber"],
)

# Drop redundant customer fields after join
step_trainer_cleaned = DropFields.apply(
    frame=joined_data,
    paths=["cust_serialNumber", "birthDay", "customerName", "email", "phone"],
)

# Write trusted step trainer dataset back to S3
sink = glue_ctx.getSink(
    connection_type="s3",
    path="s3://vs-stedi-human-balance-analytics/step_trainer/trusted/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[]
)
sink.setFormat("json")
sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="step_trainer_trusted")
sink.writeFrame(step_trainer_cleaned)


# Commit job
job.commit()
