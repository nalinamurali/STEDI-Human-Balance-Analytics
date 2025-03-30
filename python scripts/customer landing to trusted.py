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

# Script generated for node customer landing zone
customerlandingzone_node1743337441404 = glueContext.create_dynamic_frame.from_catalog(database="stedi-datalakehouse-db", table_name="customer_landing", transformation_ctx="customerlandingzone_node1743337441404")

# Script generated for node SQL Query
SqlQuery69 = '''
select * from myDataSource where
sharewithresearchasofdate is not NULL

'''
SQLQuery_node1743338237144 = sparkSqlQuery(glueContext, query = SqlQuery69, mapping = {"myDataSource":customerlandingzone_node1743337441404}, transformation_ctx = "SQLQuery_node1743338237144")

# Script generated for node customer trusted zone
EvaluateDataQuality().process_rows(frame=SQLQuery_node1743338237144, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743335837388", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customertrustedzone_node1743337584635 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1743338237144, connection_type="s3", format="json", connection_options={"path": "s3://stedi-spark-datalakehouse/customer/trusted/", "partitionKeys": []}, transformation_ctx="customertrustedzone_node1743337584635")

job.commit()