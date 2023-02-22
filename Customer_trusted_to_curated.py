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

# Script generated for node Customer Trusted
CustomerTrusted_node1676735732267 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://clydepooh/customer/trusted/"], "recurse": True},
    transformation_ctx="CustomerTrusted_node1676735732267",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="clydedb",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Join Customer
JoinCustomer_node2 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrusted_node1676735732267,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node2",
)

# Script generated for node Drop Fields
DropFields_node1676736303103 = DropFields.apply(
    frame=JoinCustomer_node2,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1676736303103",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1676736303103,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://clydepooh/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node3",
)

job.commit()
