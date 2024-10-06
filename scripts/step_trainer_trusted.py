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
customer_landing_node1728179589208 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://lawrence-datalake-project/customer/landing/"], "recurse": True}, transformation_ctx="customer_landing_node1728179589208")

# Script generated for node step_trainer_landing
step_trainer_landing_node1728179521616 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://lawrence-datalake-project/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1728179521616")

# Script generated for node step_trainer_customer_landing_join
step_trainer_customer_landing_join_node1728179814944 = Join.apply(frame1=step_trainer_landing_node1728179521616, frame2=customer_landing_node1728179589208, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="step_trainer_customer_landing_join_node1728179814944")

# Script generated for node Query Filter
SqlQuery4876 = '''
select * from myDataSource
where shareWithResearchAsOfDate <> 0
'''
QueryFilter_node1728179832022 = sparkSqlQuery(glueContext, query = SqlQuery4876, mapping = {"myDataSource":step_trainer_customer_landing_join_node1728179814944}, transformation_ctx = "QueryFilter_node1728179832022")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1728179988726 = glueContext.getSink(path="s3://lawrence-datalake-project/step_trainer/trusted/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1728179988726")
step_trainer_trusted_node1728179988726.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1728179988726.setFormat("json")
step_trainer_trusted_node1728179988726.writeFrame(QueryFilter_node1728179832022)
job.commit()