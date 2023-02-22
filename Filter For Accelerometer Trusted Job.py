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

# Script generated for node S3 bucket Customer Trusted Data
S3bucketCustomerTrustedData_node1676818865236 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://nasaccelerometerproject/customer/trusted/"],
            "recurse": True,
        },
        transformation_ctx="S3bucketCustomerTrustedData_node1676818865236",
    )
)

# Script generated for node S3 bucket All Accelerometer Data
S3bucketAllAccelerometerData_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nasaccelerometerproject/sources/accelerometer/"],
        "recurse": True,
    },
    transformation_ctx="S3bucketAllAccelerometerData_node1",
)

# Script generated for node Join
Join_node1676818910393 = Join.apply(
    frame1=S3bucketAllAccelerometerData_node1,
    frame2=S3bucketCustomerTrustedData_node1676818865236,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1676818910393",
)

# Script generated for node Accelerometer Data Filter
AccelerometerDataFilter_node2 = ApplyMapping.apply(
    frame=Join_node1676818910393,
    mappings=[
        ("user", "string", "user", "string"),
        ("timeStamp", "long", "timeStamp", "long"),
        ("x", "double", "x", "double"),
        ("y", "double", "y", "double"),
        ("z", "double", "z", "double"),
    ],
    transformation_ctx="AccelerometerDataFilter_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=AccelerometerDataFilter_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://nasaccelerometerproject/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
