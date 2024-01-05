import boto3
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

client = boto3.client("glue")

tables = [
    x["Name"]
    for x in client.get_tables(DatabaseName="tickit-metastore")["TableList"]
]
abbreviations = [x.split("_")[-1] for x in tables]  # Table Names Abbreviations
BUCKET_NAME = "tickit-lake"
GLUE_DB = "tickit-metastore"

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


for table, abbr in zip(tables, abbreviations):
    DataCatalogtable_node = glueContext.create_dynamic_frame.from_catalog(
        database=GLUE_DB,
        table_name=table,
        transformation_ctx="DataCatalogtable_node1",
    )

    # Script generated for node S3 bucket
    S3bucket_node3 = glueContext.getSink(
        path=f"s3://{BUCKET_NAME}/silver/{abbr}/",
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[],
        enableUpdateCatalog=True,
        transformation_ctx="S3bucket_node3",
    )
    S3bucket_node3.setCatalogInfo(
        catalogDatabase=GLUE_DB, catalogTableName=f"refined_tickit_public_{abbr}"
    )
    S3bucket_node3.setFormat("glueparquet")
    S3bucket_node3.writeFrame(DataCatalogtable_node)

job.commit()