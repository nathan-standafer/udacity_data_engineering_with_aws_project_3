import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket Customer Curated
S3bucketCustomerCurated_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nasaccelerometerproject/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="S3bucketCustomerCurated_node1",
)

# Script generated for node S3 bucket step_trainer data stream
S3bucketstep_trainerdatastream_node1676822869412 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://nasaccelerometerproject/sources/step_trainer/"],
            "recurse": True,
        },
        transformation_ctx="S3bucketstep_trainerdatastream_node1676822869412",
    )
)

# Script generated for node Join
Join_node1676822912477 = Join.apply(
    frame1=S3bucketCustomerCurated_node1,
    frame2=S3bucketstep_trainerdatastream_node1676822869412,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1676822912477",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=Join_node1676822912477,
    mappings=[
        ("serialNumber", "string", "serialNumber", "string"),
        ("sensorReadingTime", "long", "sensorReadingTime", "long"),
        ("`.serialNumber`", "string", "`.serialNumber`", "string"),
        ("distanceFromObject", "int", "distanceFromObject", "int"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1676822979120 = DynamicFrame.fromDF(
    ApplyMapping_node2.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1676822979120",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1676822979120,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://nasaccelerometerproject/step_trainer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
