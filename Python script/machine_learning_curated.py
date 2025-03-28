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

# Script generated for node accelerometer trusted node
accelerometertrustednode_node1743134238203 = glueContext.create_dynamic_frame.from_catalog(database="uda-datalake-db", table_name="accelerometer_trusted", transformation_ctx="accelerometertrustednode_node1743134238203")

# Script generated for node step trainer trusted node
steptrainertrustednode_node1743134237089 = glueContext.create_dynamic_frame.from_catalog(database="uda-datalake-db", table_name="step_trainer_trusred", transformation_ctx="steptrainertrustednode_node1743134237089")

# Script generated for node Join
Join_node1743134331222 = Join.apply(frame1=accelerometertrustednode_node1743134238203, frame2=steptrainertrustednode_node1743134237089, keys1=["timestamp"], keys2=["sensorreadingtime"], transformation_ctx="Join_node1743134331222")

# Script generated for node ML curated
EvaluateDataQuality().process_rows(frame=Join_node1743134331222, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743134227210", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MLcurated_node1743134351417 = glueContext.write_dynamic_frame.from_options(frame=Join_node1743134331222, connection_type="s3", format="json", connection_options={"path": "s3://uda-spark-datalake/step-trainer/curated/", "partitionKeys": []}, transformation_ctx="MLcurated_node1743134351417")

job.commit()