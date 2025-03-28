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

# Script generated for node accelerometer landing zone
accelerometerlandingzone_node1743050461270 = glueContext.create_dynamic_frame.from_catalog(database="uda-datalake-db", table_name="accelerometer_landing", transformation_ctx="accelerometerlandingzone_node1743050461270")

# Script generated for node customer trusted zone
customertrustedzone_node1743050459721 = glueContext.create_dynamic_frame.from_catalog(database="uda-datalake-db", table_name="customer_trusted", transformation_ctx="customertrustedzone_node1743050459721")

# Script generated for node Join
Join_node1743050540936 = Join.apply(frame1=accelerometerlandingzone_node1743050461270, frame2=customertrustedzone_node1743050459721, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1743050540936")

# Script generated for node SQL Query
SqlQuery1313 = '''
select serialNumber,shareWithPublicAsOfDate,birthday,
registrationDate,shareWithResearchAsOfDate,customerName,
lastUpdateDate,email,phone,shareWithFriendsAsOfDate
from myDataSource

'''
SQLQuery_node1743050588417 = sparkSqlQuery(glueContext, query = SqlQuery1313, mapping = {"myDataSource":Join_node1743050540936}, transformation_ctx = "SQLQuery_node1743050588417")

# Script generated for node customer curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1743050588417, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743043855747", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customercurated_node1743050746766 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1743050588417, connection_type="s3", format="json", connection_options={"path": "s3://uda-spark-datalake/customer/curated/", "partitionKeys": []}, transformation_ctx="customercurated_node1743050746766")

job.commit()