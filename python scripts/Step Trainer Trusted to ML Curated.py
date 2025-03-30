import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node Step Trainer Trusted Zone
StepTrainerTrustedZone_node1743342169391 = glueContext.create_dynamic_frame.from_catalog(database="stedi-datalakehouse-db", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrustedZone_node1743342169391")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1743342145605 = glueContext.create_dynamic_frame.from_catalog(database="stedi-datalakehouse-db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1743342145605")

# Script generated for node SQL Query
SqlQuery55 = '''
select * from stt join att on stt.sensorreadingtime = att.timestamp

'''
SQLQuery_node1743342533781 = sparkSqlQuery(glueContext, query = SqlQuery55, mapping = {"stt":StepTrainerTrustedZone_node1743342169391, "att":AccelerometerTrusted_node1743342145605}, transformation_ctx = "SQLQuery_node1743342533781")

# Script generated for node Drop Duplicates
DropDuplicates_node1743342681948 =  DynamicFrame.fromDF(SQLQuery_node1743342533781.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1743342681948")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1743342681948, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743342140425", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1743342216519 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1743342681948, connection_type="s3", format="json", connection_options={"path": "s3://stedi-spark-datalakehouse/step_trainer/curated/", "partitionKeys": []}, transformation_ctx="MachineLearningCurated_node1743342216519")

job.commit()