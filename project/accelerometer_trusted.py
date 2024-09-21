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

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1726890267705 = glueContext.create_dynamic_frame.from_catalog(database="215215215", table_name="customer_trusted", transformation_ctx="CustomerTrustedZone_node1726890267705")

# Script generated for node Accelerometer Landing Zone
AccelerometerLandingZone_node1726890266425 = glueContext.create_dynamic_frame.from_catalog(database="215215215", table_name="accelerometer_landing", transformation_ctx="AccelerometerLandingZone_node1726890266425")

# Script generated for node Customer Privacy Filter
SqlQuery4458 = '''
select a.* from customer_trusted c
    join accelerometer_landing a
        on c.email = a.user
'''
CustomerPrivacyFilter_node1726915479366 = sparkSqlQuery(glueContext, query = SqlQuery4458, mapping = {"customer_trusted":CustomerTrustedZone_node1726890267705, "accelerometer_landing":AccelerometerLandingZone_node1726890266425}, transformation_ctx = "CustomerPrivacyFilter_node1726915479366")

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1726890282446 = glueContext.getSink(path="s3://215215215/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrustedZone_node1726890282446")
AccelerometerTrustedZone_node1726890282446.setCatalogInfo(catalogDatabase="215215215",catalogTableName="accelerometer_trusted")
AccelerometerTrustedZone_node1726890282446.setFormat("json")
AccelerometerTrustedZone_node1726890282446.writeFrame(CustomerPrivacyFilter_node1726915479366)
job.commit()