import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *

# --- Initialize Spark/Glue context ---
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

# --- Read Customer Trusted data from S3 ---
customer_trusted_df = glue_ctx.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://vs-stedi-human-balance-analytics/customer/trusted/"],
        "recurse": True,
    },
    format_options={"multiline": True},
    transformation_ctx="CustomerTrusted",
)

# --- Read Accelerometer Landing data from S3 ---
accelerometer_landing_df = glue_ctx.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://vs-stedi-human-balance-analytics/accelerometer/landing/"],
        "recurse": True,
    },
    format_options={"multiline": False},
    transformation_ctx="AccelerometerLanding",
)

# --- Join Customer & Accelerometer datasets ---
joined_frames = Join.apply(
    frame1=customer_trusted_df,
    frame2=accelerometer_landing_df,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="CustomerAccelerometerJoin",
)

# --- Drop unnecessary fields to protect PII ---
cleaned_df = DropFields.apply(
    frame=joined_frames,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropSensitiveColumns",
)

# --- Write Accelerometer Trusted dataset back to S3 ---
glue_ctx.write_dynamic_frame.from_options(
    frame=cleaned_df,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://vs-stedi-human-balance-analytics/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted",
)

job.commit()
