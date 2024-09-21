import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node Customer Landing Zone
CustomerLandingZone_node1726884629860 = glueContext.create_dynamic_frame.from_catalog(database="215215215", table_name="landing_zone", transformation_ctx="CustomerLandingZone_node1726884629860")

# Script generated for node Privacy Filter
SqlQuery4554 = '''
select * from customer_landing
where sharewithresearchasofdate is not null
'''
PrivacyFilter_node1726893520786 = sparkSqlQuery(glueContext, query = SqlQuery4554, mapping = {"customer_landing":CustomerLandingZone_node1726884629860}, transformation_ctx = "PrivacyFilter_node1726893520786")

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1726840534892 = glueContext.getSink(path="s3://215215215/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrustedZone_node1726840534892")
CustomerTrustedZone_node1726840534892.setCatalogInfo(catalogDatabase="215215215",catalogTableName="customer_trusted")
CustomerTrustedZone_node1726840534892.setFormat("json")
CustomerTrustedZone_node1726840534892.writeFrame(PrivacyFilter_node1726893520786)
job.commit()