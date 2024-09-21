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

# Script generated for node Customer Trusted
CustomerTrusted_node1726890267705 = glueContext.create_dynamic_frame.from_catalog(database="215215215", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1726890267705")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1726890266425 = glueContext.create_dynamic_frame.from_catalog(database="215215215", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1726890266425")

# Script generated for node Join
Join_node1726890279988 = Join.apply(frame1=CustomerTrusted_node1726890267705, frame2=AccelerometerLanding_node1726890266425, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1726890279988")

# Script generated for node SQL Query
SqlQuery4123 = '''
select distinct customername, email, phone, birthday, 
serialnumber, registrationdate, lastupdatedate, 
sharewithresearchasofdate, sharewithpublicasofdate,
sharewithfriendsasofdate from joined_result
'''
SQLQuery_node1726895182781 = sparkSqlQuery(glueContext, query = SqlQuery4123, mapping = {"joined_result":Join_node1726890279988}, transformation_ctx = "SQLQuery_node1726895182781")

# Script generated for node Customer Curated
CustomerCurated_node1726890282446 = glueContext.getSink(path="s3://215215215/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1726890282446")
CustomerCurated_node1726890282446.setCatalogInfo(catalogDatabase="215215215",catalogTableName="customer_curated")
CustomerCurated_node1726890282446.setFormat("json")
CustomerCurated_node1726890282446.writeFrame(SQLQuery_node1726895182781)
job.commit()