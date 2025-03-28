import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1743045409285 = glueContext.create_dynamic_frame.from_catalog(database="uda-datalake-db", table_name="customer_trusted", transformation_ctx="CustomerTrustedZone_node1743045409285")

# Script generated for node Accelerometer Landing zone
AccelerometerLandingzone_node1743045411750 = glueContext.create_dynamic_frame.from_catalog(database="uda-datalake-db", table_name="accelerometer_landing", transformation_ctx="AccelerometerLandingzone_node1743045411750")

# Script generated for node Join
Join_node1743045470040 = Join.apply(frame1=AccelerometerLandingzone_node1743045411750, frame2=CustomerTrustedZone_node1743045409285, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1743045470040")

# Script generated for node Accelerometer Trusted Zone
EvaluateDataQuality().process_rows(frame=Join_node1743045470040, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743043855747", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrustedZone_node1743045511072 = glueContext.write_dynamic_frame.from_options(frame=Join_node1743045470040, connection_type="s3", format="json", connection_options={"path": "s3://uda-spark-datalake/accelerometer/trusted/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="AccelerometerTrustedZone_node1743045511072")

job.commit()