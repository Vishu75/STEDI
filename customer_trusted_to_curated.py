import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Join, DropFields

# Initialize Glue job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

# --- Load Trusted Customer Data ---
customer_trusted = glue_ctx.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    format_options={"multiline": False},
    connection_options={
        "paths": ["s3://vs-stedi-human-balance-analytics/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted"
)

# --- Load Trusted Accelerometer Data ---
accel_trusted = glue_ctx.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    format_options={"multiline": False},
    connection_options={
        "paths": ["s3://vs-stedi-human-balance-analytics/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accel_trusted"
)

# --- Join Customers with Accelerometer Records ---
joined_df = Join.apply(
    frame1=customer_trusted,
    frame2=accel_trusted,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="joined_df"
)

# --- Remove unnecessary accelerometer fields ---
cleaned_df = DropFields.apply(
    frame=joined_df,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="cleaned_df"
)

# --- Drop duplicate customer records ---
unique_customers = DynamicFrame.fromDF(
    cleaned_df.toDF().dropDuplicates(),
    glue_ctx,
    "unique_customers"
)

# --- Save curated customers into S3 ---
glue_ctx.write_dynamic_frame.from_options(
    frame=unique_customers,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://vs-stedi-human-balance-analytics/customer/curated/",
        "partitionKeys": []
    },
    transformation_ctx="customer_curated_sink"
)

job.commit()
