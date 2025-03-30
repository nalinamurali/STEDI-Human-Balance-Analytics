import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

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

# Script generated for node Step Trainer Landing zone
StepTrainerLandingzone_node1743341452697 = glueContext.create_dynamic_frame.from_catalog(database="stedi-datalakehouse-db", table_name="step_trainer_landing", transformation_ctx="StepTrainerLandingzone_node1743341452697")

# Script generated for node Customer curated zone
Customercuratedzone_node1743341425713 = glueContext.create_dynamic_frame.from_catalog(database="stedi-datalakehouse-db", table_name="customer_curated", transformation_ctx="Customercuratedzone_node1743341425713")

# Script generated for node SQL Query
SqlQuery56 = '''
select * from st join cc on st.serialNumber = cc.serialNumber

'''
SQLQuery_node1743341484881 = sparkSqlQuery(glueContext, query = SqlQuery56, mapping = {"st":StepTrainerLandingzone_node1743341452697, "cc":Customercuratedzone_node1743341425713}, transformation_ctx = "SQLQuery_node1743341484881")

# Script generated for node Drop Fields
DropFields_node1743341727792 = DropFields.apply(frame=SQLQuery_node1743341484881, paths=["`.customerName`", "birthDay", "shareWithPublicAsOfDate", "`.email`", "shareWithResearchAsOfDate", "registrationDate", "customerName", "`.phone`", "`.shareWithPublicAsOfDate`", "shareWithFriendsAsOfDate", "`.birthDay`", "`.lastUpdateDate`", "`.shareWithFriendsAsOfDate`", "`.registrationDate`", "`.serialNumber`", "email", "lastUpdateDate", "`.shareWithResearchAsOfDate`", "phone"], transformation_ctx="DropFields_node1743341727792")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1743341727792, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743341419125", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1743341665018 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1743341727792, connection_type="s3", format="json", connection_options={"path": "s3://stedi-spark-datalakehouse/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="StepTrainerTrusted_node1743341665018")

job.commit()