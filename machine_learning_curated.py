import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.transforms import Join, DropFields

# Initialize Glue Job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

# --- Load Step Trainer Trusted data ---
step_trainer_trusted = glue_ctx.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    format_options={"multiline": False},
    connection_options={
        "paths": ["s3://vs-stedi-human-balance-analytics/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted"
)

# --- Load Curated Customers data ---
customers_curated = glue_ctx.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    format_options={"multiline": False},
    connection_options={
        "paths": ["s3://vs-stedi-human-balance-analytics/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customers_curated"
)

# --- Load Accelerometer Trusted data ---
accelerometer_trusted = glue_ctx.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    format_options={"multiline": False},
    connection_options={
        "paths": ["s3://vs-stedi-human-balance-analytics/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted"
)

# --- Join Step Trainer and Accelerometer readings (on timestamp) ---
step_with_accel = Join.apply(
    frame1=accelerometer_trusted,
    frame2=step_trainer_trusted,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="step_with_accel"
)

# --- Join above result with curated customers (on email <-> user) ---
full_joined = Join.apply(
    frame1=customers_curated,
    frame2=step_with_accel,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="full_joined"
)

# --- Remove unnecessary personal and duplicate fields ---
final_dataset = DropFields.apply(
    frame=full_joined,
    paths=[
        "customerName",
        "email",
        "phone",
        "birthDay",
        "lastUpdateDate",
        "registrationDate",
        "shareWithFriendsAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "`.serialNumber`"
    ],
    transformation_ctx="final_dataset"
)

# --- Write final machine learning curated dataset to S3 ---
glue_ctx.write_dynamic_frame.from_options(
    frame=final_dataset,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://vs-stedi-human-balance-analytics/machine_learning_curated/",
        "partitionKeys": []
    },
    transformation_ctx="ml_curated_sink"
)

job.commit()
