import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, substring

# Para rodar local precisamos comentar essa linha pois não temos esse parâmetro
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
#job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
dynami_frame = (
    glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="csv",
        connection_options={"paths": ["s3://files-joao/SF/"], "recurse": False},
        transformation_ctx="dynami_frame"))

data_frame = dynami_frame.toDF().withColumnRenamed("col0", "trancode_mainframe")
data_frame.printSchema()
df = (data_frame.withColumn('custodia',  col('trancode_mainframe').substr(0, 2))
      .withColumn('codigo_dominio',  col('trancode_mainframe').substr(3, 2))
      .withColumn('id_operacao',  col('trancode_mainframe').substr(5, 8))
      .withColumn('id_carga',  col('trancode_mainframe').substr(13, 15))
      .withColumn('trancode',  col('trancode_mainframe').substr(28, 100000)))


df.show()



job.commit()