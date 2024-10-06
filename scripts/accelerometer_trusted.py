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

# Script generated for node accelerometer_landing
accelerometer_landing_node1728178457924 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1728178457924")

# Script generated for node customer_landing
customer_landing_node1728178521154 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_landing", transformation_ctx="customer_landing_node1728178521154")

# Script generated for node accelerometer_customer_landing_join
accelerometer_customer_landing_join_node1728178548393 = Join.apply(frame1=accelerometer_landing_node1728178457924, frame2=customer_landing_node1728178521154, keys1=["user"], keys2=["email"], transformation_ctx="accelerometer_customer_landing_join_node1728178548393")

# Script generated for node Research Filter
SqlQuery4993 = '''
select * from myDataSource
where shareWithResearchAsOfDate <> 0;
'''
ResearchFilter_node1728178612328 = sparkSqlQuery(glueContext, query = SqlQuery4993, mapping = {"myDataSource":accelerometer_customer_landing_join_node1728178548393}, transformation_ctx = "ResearchFilter_node1728178612328")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1728178673014 = glueContext.getSink(path="s3://lawrence-datalake-project/accelerometer/trusted/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1728178673014")
accelerometer_trusted_node1728178673014.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1728178673014.setFormat("json")
accelerometer_trusted_node1728178673014.writeFrame(ResearchFilter_node1728178612328)
job.commit()