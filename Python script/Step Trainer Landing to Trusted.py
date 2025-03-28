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

# Script generated for node Customer Curated Zone
CustomerCuratedZone_node1743072458272 = glueContext.create_dynamic_frame.from_catalog(database="uda-datalake-db", table_name="customer_curated", transformation_ctx="CustomerCuratedZone_node1743072458272")

# Script generated for node Step Trainer Landing zone
StepTrainerLandingzone_node1743072480853 = glueContext.create_dynamic_frame.from_catalog(database="uda-datalake-db", table_name="step_trainer_landing", transformation_ctx="StepTrainerLandingzone_node1743072480853")

# Script generated for node Join
Join_node1743072523031 = Join.apply(frame1=StepTrainerLandingzone_node1743072480853, frame2=CustomerCuratedZone_node1743072458272, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="Join_node1743072523031")

# Script generated for node Drop Fields
DropFields_node1743073608731 = DropFields.apply(frame=Join_node1743072523031, paths=["sensorreadingtime", "serialnumber", "distancefromobject"], transformation_ctx="DropFields_node1743073608731")

# Script generated for node Step Trainer Trusted zone
EvaluateDataQuality().process_rows(frame=DropFields_node1743073608731, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743071550585", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrustedzone_node1743072622742 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1743073608731, connection_type="s3", format="json", connection_options={"path": "s3://uda-spark-datalake/step-trainer/trusted/", "partitionKeys": []}, transformation_ctx="StepTrainerTrustedzone_node1743072622742")

job.commit()