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

# Script generated for node S3 bucket Step Trainer Curated
S3bucketStepTrainerCurated_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nasaccelerometerproject/step_trainer/curated/"],
        "recurse": True,
    },
    transformation_ctx="S3bucketStepTrainerCurated_node1",
)

# Script generated for node S3 bucket Customer cruated
S3bucketCustomercruated_node1676907557737 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://nasaccelerometerproject/customer/curated/"],
            "recurse": True,
        },
        transformation_ctx="S3bucketCustomercruated_node1676907557737",
    )
)

# Script generated for node S3 bucket Accelerometer Data Trusted
S3bucketAccelerometerDataTrusted_node1676907273871 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://nasaccelerometerproject/accelerometer/trusted/"],
            "recurse": True,
        },
        transformation_ctx="S3bucketAccelerometerDataTrusted_node1676907273871",
    )
)

# Script generated for node Join Trainer-Customer
JoinTrainerCustomer_node1676907400296 = Join.apply(
    frame1=S3bucketStepTrainerCurated_node1,
    frame2=S3bucketCustomercruated_node1676907557737,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="JoinTrainerCustomer_node1676907400296",
)

# Script generated for node Join trainer-customer-accelerometer
Jointrainercustomeraccelerometer_node1676907655982 = Join.apply(
    frame1=JoinTrainerCustomer_node1676907400296,
    frame2=S3bucketAccelerometerDataTrusted_node1676907273871,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Jointrainercustomeraccelerometer_node1676907655982",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=Jointrainercustomeraccelerometer_node1676907655982,
    mappings=[
        ("serialNumber", "string", "serialNumber", "string"),
        ("sensorReadingTime", "long", "sensorReadingTime", "long"),
        ("distanceFromObject", "int", "distanceFromObject", "int"),
        ("`.serialNumber`", "string", "`.serialNumber`", "string"),
        ("shareWithPublicAsOfDate", "long", "shareWithPublicAsOfDate", "long"),
        ("user", "string", "user", "string"),
        ("timeStamp", "long", "timeStamp", "long"),
        ("x", "double", "x", "double"),
        ("y", "double", "y", "double"),
        ("z", "double", "z", "double"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1676907730004 = DynamicFrame.fromDF(
    ApplyMapping_node2.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1676907730004",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1676907730004,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://nasaccelerometerproject/machinelearning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
