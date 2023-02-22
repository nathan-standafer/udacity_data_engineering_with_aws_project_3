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

# Script generated for node S3 Step trainer Trusted
S3SteptrainerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nasaccelerometerproject/step_trainer/curated/"],
        "recurse": True,
    },
    transformation_ctx="S3SteptrainerTrusted_node1",
)

# Script generated for node S3 Accelerometer Trusted
S3AccelerometerTrusted_node1677024525542 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://nasaccelerometerproject/accelerometer/trusted/"],
            "recurse": True,
        },
        transformation_ctx="S3AccelerometerTrusted_node1677024525542",
    )
)

# Script generated for node Join StepTrainer - Customer
JoinStepTrainerCustomer_node1676927486799 = Join.apply(
    frame1=S3SteptrainerTrusted_node1,
    frame2=S3AccelerometerTrusted_node1677024525542,
    keys1=["serialNumber", "sensorReadingTime"],
    keys2=["serialNumber", "timeStamp"],
    transformation_ctx="JoinStepTrainerCustomer_node1676927486799",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=JoinStepTrainerCustomer_node1676927486799,
    mappings=[
        ("serialNumber", "string", "serialNumber", "string"),
        ("sensorReadingTime", "bigint", "sensorReadingTime", "long"),
        ("distanceFromObject", "int", "distanceFromObject", "int"),
        ("`.serialNumber`", "string", "`.serialNumber`", "string"),
        ("z", "double", "z", "double"),
        ("timeStamp", "long", "timeStamp", "long"),
        ("customerName", "string", "customerName", "string"),
        ("user", "string", "user", "string"),
        ("y", "double", "y", "double"),
        ("x", "double", "x", "double"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1676927671974 = DynamicFrame.fromDF(
    ApplyMapping_node2.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1676927671974",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1676927671974,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://nasaccelerometerproject/machinelearning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
