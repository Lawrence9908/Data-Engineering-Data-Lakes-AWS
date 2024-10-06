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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1728181007364 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1728181007364")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1728180587183 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1728180587183")

# Script generated for node Join
Join_node1728181066507 = Join.apply(frame1=accelerometer_trusted_node1728180587183, frame2=step_trainer_trusted_node1728181007364, keys1=["timestamp"], keys2=["sensorreadingtime"], transformation_ctx="Join_node1728181066507")

# Script generated for node Query Filter
SqlQuery4909 = '''
select * from myDataSource
where shareWithResearchAsOfDate <> 0
'''
QueryFilter_node1728181157928 = sparkSqlQuery(glueContext, query = SqlQuery4909, mapping = {"myDataSource":Join_node1728181066507}, transformation_ctx = "QueryFilter_node1728181157928")

# Script generated for node machine_learning_curated
machine_learning_curated_node1728181267087 = glueContext.getSink(path="s3://lawrence-datalake-project/step_trainer/curated/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1728181267087")
machine_learning_curated_node1728181267087.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="machine_learning_curated")
machine_learning_curated_node1728181267087.setFormat("json")
machine_learning_curated_node1728181267087.writeFrame(QueryFilter_node1728181157928)
job.commit()