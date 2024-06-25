import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import concat_ws
from awsglue import DynamicFrame

# Get job arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize the job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read data from S3
Amazon53_node1697982521889 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": "", 
        "withHeader": True, 
        "separator": ""
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "recurse": True, 
        "paths": ["s3://reddit-engineering/raw/reddit_20240602.csv"]
    },
    transformation_ctx="Amazon53_node1697982521889"
)

# Convert DynamicFrame to DataFrame
df = Amazon53_node1697982521889.toDF()

# Concatenate the three columns into a single column
df_combined = df.withColumn('ESS_updated', concat_ws(" ", df['edited'], df['spoiler'], df['stickied']))

# Drop the original columns
df_combined = df_combined.drop('edited', 'spoiler', 'stickied')

# Convert back to DynamicFrame
S3bucket_node_combined = DynamicFrame.fromDF(df_combined, glueContext, "S3bucket_node_combined")

# Write the transformed data back to S3
glueContext.write_dynamic_frame.from_options(
    frame=S3bucket_node_combined,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://reddit-engineering/transformed/",
        "partitionKeys": []
    },
    transformation_ctx="Amazon53_node1697982526335"
)

# Commit the job
job.commit()
