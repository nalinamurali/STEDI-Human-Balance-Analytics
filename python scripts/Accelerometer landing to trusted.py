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

# Script generated for node customer trusted zone
customertrustedzone_node1743338642510 = glueContext.create_dynamic_frame.from_catalog(database="stedi-datalakehouse-db", table_name="customer_trusted", transformation_ctx="customertrustedzone_node1743338642510")

# Script generated for node Accelerometer Landing zone
AccelerometerLandingzone_node1743338685800 = glueContext.create_dynamic_frame.from_catalog(database="stedi-datalakehouse-db", table_name="accelerometer_landing", transformation_ctx="AccelerometerLandingzone_node1743338685800")

# Script generated for node Join
Join_node1743338708807 = Join.apply(frame1=AccelerometerLandingzone_node1743338685800, frame2=customertrustedzone_node1743338642510, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1743338708807")

# Script generated for node Accelerometer Landing to Trusted
EvaluateDataQuality().process_rows(frame=Join_node1743338708807, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743338631222", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerLandingtoTrusted_node1743338763375 = glueContext.write_dynamic_frame.from_options(frame=Join_node1743338708807, connection_type="s3", format="json", connection_options={"path": "s3://stedi-spark-datalakehouse/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="AccelerometerLandingtoTrusted_node1743338763375")

job.commit()