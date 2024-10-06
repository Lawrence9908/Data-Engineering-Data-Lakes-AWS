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

# Script generated for node customer_landing
customer_landing_node1728178249855 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_landing", transformation_ctx="customer_landing_node1728178249855")

# Script generated for node Query Filter
SqlQuery5001 = '''
select * from myDataSource
where shareWithResearchAsOfDate <> 0

'''
QueryFilter_node1728178159613 = sparkSqlQuery(glueContext, query = SqlQuery5001, mapping = {"myDataSource":customer_landing_node1728178249855}, transformation_ctx = "QueryFilter_node1728178159613")

# Script generated for node customer_trusted
customer_trusted_node1728178137804 = glueContext.getSink(path="s3://lawrence-datalake-project/customer/trusted/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1728178137804")
customer_trusted_node1728178137804.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_trusted")
customer_trusted_node1728178137804.setFormat("json")
customer_trusted_node1728178137804.writeFrame(QueryFilter_node1728178159613)
job.commit()