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

# Script generated for node customers_trusted
customers_trusted_node1728178941419 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="customers_trusted_node1728178941419")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1728178950511 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1728178950511")

# Script generated for node Renamed keys for customer_accelerometer_trusted_join
Renamedkeysforcustomer_accelerometer_trusted_join_node1728179098745 = ApplyMapping.apply(frame=accelerometer_trusted_node1728178950511, mappings=[("serialnumber", "string", "accelerometer_serialnumber", "string"), ("z", "double", "accelerometer_z", "double"), ("birthday", "string", "accelerometer_birthday", "string"), ("sharewithpublicasofdate", "long", "accelerometer_sharewithpublicasofdate", "long"), ("sharewithresearchasofdate", "long", "accelerometer_sharewithresearchasofdate", "long"), ("registrationdate", "long", "accelerometer_registrationdate", "long"), ("customername", "string", "accelerometer_customername", "string"), ("user", "string", "accelerometer_user", "string"), ("sharewithfriendsasofdate", "long", "accelerometer_sharewithfriendsasofdate", "long"), ("y", "double", "accelerometer_y", "double"), ("x", "double", "accelerometer_x", "double"), ("timestamp", "long", "accelerometer_timestamp", "long"), ("email", "string", "accelerometer_email", "string"), ("lastupdatedate", "long", "accelerometer_lastupdatedate", "long"), ("phone", "string", "accelerometer_phone", "string")], transformation_ctx="Renamedkeysforcustomer_accelerometer_trusted_join_node1728179098745")

# Script generated for node customer_accelerometer_trusted_join
customer_accelerometer_trusted_join_node1728179018489 = Join.apply(frame1=customers_trusted_node1728178941419, frame2=Renamedkeysforcustomer_accelerometer_trusted_join_node1728179098745, keys1=["email"], keys2=["accelerometer_user"], transformation_ctx="customer_accelerometer_trusted_join_node1728179018489")

# Script generated for node Research Filter
SqlQuery4874 = '''
select * from myDataSource
where shareWithResearchAsOfDate <> 0 and
accelerometer_user is not null
'''
ResearchFilter_node1728179149721 = sparkSqlQuery(glueContext, query = SqlQuery4874, mapping = {"myDataSource":customer_accelerometer_trusted_join_node1728179018489}, transformation_ctx = "ResearchFilter_node1728179149721")

# Script generated for node customers_curated
customers_curated_node1728179364784 = glueContext.getSink(path="s3://lawrence-datalake-project/customer/curated/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customers_curated_node1728179364784")
customers_curated_node1728179364784.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customers_curated")
customers_curated_node1728179364784.setFormat("glueparquet", compression="snappy")
customers_curated_node1728179364784.writeFrame(ResearchFilter_node1728179149721)
job.commit()