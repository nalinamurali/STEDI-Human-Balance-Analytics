import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node customer landing
customerlanding_node1742533584891 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://uda-spark-datalake/customer/landing/"], "recurse": True}, transformation_ctx="customerlanding_node1742533584891")

# Script generated for node PrivacyFilter
PrivacyFilter_node1742533606041 = Filter.apply(frame=customerlanding_node1742533584891, f=lambda row: (not(row["sharewithresearchasofdate"] == 0)), transformation_ctx="PrivacyFilter_node1742533606041")

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1742533613297 = glueContext.write_dynamic_frame.from_options(frame=PrivacyFilter_node1742533606041, connection_type="s3", format="json", connection_options={"path": "s3://uda-spark-datalake/customer/trusted/", "partitionKeys": []}, transformation_ctx="CustomerTrustedZone_node1742533613297")

job.commit()