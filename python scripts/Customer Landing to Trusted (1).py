import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
import re

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

# Script generated for node Customer Landing Zone
CustomerLandingZone_node1743335866806 = glueContext.create_dynamic_frame.from_catalog(database="stedi-datalakehouse-db", table_name="customer_landing", transformation_ctx="CustomerLandingZone_node1743335866806")

# Script generated for node Privacy Filter
PrivacyFilter_node1743335904512 = Filter.apply(frame=CustomerLandingZone_node1743335866806, f=lambda row: (not(row["sharewithresearchasofdate"] == 0)), transformation_ctx="PrivacyFilter_node1743335904512")

# Script generated for node Customer Landing to Trusted
EvaluateDataQuality().process_rows(frame=PrivacyFilter_node1743335904512, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743335837388", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerLandingtoTrusted_node1743335957887 = glueContext.write_dynamic_frame.from_options(frame=PrivacyFilter_node1743335904512, connection_type="s3", format="json", connection_options={"path": "s3://stedi-spark-datalakehouse/customer/trusted/", "partitionKeys": []}, transformation_ctx="CustomerLandingtoTrusted_node1743335957887")

job.commit()