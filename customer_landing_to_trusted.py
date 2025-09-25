import sys
import re
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.transforms import Filter

# Initialize Glue job and context
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# --- Load customer landing data from S3 ---
customer_landing_df = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    format_options={"multiline": False},
    connection_options={
        "paths": ["s3://vs-stedi-human-balance-analytics/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="customer_landing_df"
)

# --- Keep only records where customer consented to research ---
consented_customers = Filter.apply(
    frame=customer_landing_df,
    f=lambda record: record["shareWithResearchAsOfDate"] not in (0, None),
    transformation_ctx="consented_customers"
)

# --- Write trusted customer data back to S3 ---
glue_context.write_dynamic_frame.from_options(
    frame=consented_customers,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://vs-stedi-human-balance-analytics/customer/trusted/",
        "partitionKeys": []
    },
    transformation_ctx="customer_trusted_sink"
)

job.commit()
