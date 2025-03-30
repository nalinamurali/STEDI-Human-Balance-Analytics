import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node customer trusted zone
customertrustedzone_node1743339508315 = glueContext.create_dynamic_frame.from_catalog(database="stedi-datalakehouse-db", table_name="customer_trusted", transformation_ctx="customertrustedzone_node1743339508315")

# Script generated for node accelerometer_trusted zone
accelerometer_trustedzone_node1743339480269 = glueContext.create_dynamic_frame.from_catalog(database="stedi-datalakehouse-db", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trustedzone_node1743339480269")

# Script generated for node Join
Join_node1743339529632 = Join.apply(frame1=customertrustedzone_node1743339508315, frame2=accelerometer_trustedzone_node1743339480269, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1743339529632")

# Script generated for node Drop Fields
DropFields_node1743340806567 = DropFields.apply(frame=Join_node1743339529632, paths=["z", "y", "user", "x", "timestamp"], transformation_ctx="DropFields_node1743340806567")

# Script generated for node Drop Duplicates
DropDuplicates_node1743341000628 =  DynamicFrame.fromDF(DropFields_node1743340806567.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1743341000628")

# Script generated for node Customer trusted to curated
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1743341000628, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743339468861", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Customertrustedtocurated_node1743339621814 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1743341000628, connection_type="s3", format="json", connection_options={"path": "s3://stedi-spark-datalakehouse/customer/curated/", "partitionKeys": []}, transformation_ctx="Customertrustedtocurated_node1743339621814")

job.commit()