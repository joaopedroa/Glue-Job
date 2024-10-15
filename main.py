import sys

from app.core.enums.dominio import Dominio
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, substring, lit, udf
from pyspark.sql.types import StringType

def extract(path, transformation_ctx):
    return (
    glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="csv",
        connection_options={"paths": [path], "recurse": False},
        transformation_ctx=transformation_ctx))

@udf(returnType=StringType())
def processar_trancode_de_dados_udf(trancode, codigo_dominio, codigo_identificador_carga):
    dominio = Dominio.get_instance(codigo_dominio)
    return dominio.choose_method(trancode, codigo_identificador_carga).to_json()

def transform_headers(data_frame):
    column_base = 'trancode_mainframe'
    return (data_frame
            .withColumn('custodia',  col(column_base).substr(0, 2))
            .withColumn('codigo_dominio',  col(column_base).substr(3, 2).cast("Integer"))
            .withColumn('id_operacao',  col(column_base).substr(5, 8))
            .withColumn('codigo_identificador_carga',  col(column_base).substr(13, 15))
            .withColumn('trancode',  col(column_base).substr(28, 100000))
            .withColumn('dados_dominio',  processar_trancode_de_dados_udf(col(column_base).substr(28, 100000), col(column_base).substr(3, 2).cast("Integer"), col(column_base).substr(13, 15)))
            )


# args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
#job.init(args['JOB_NAME'], args)

# Extract
dynamic_frame = extract("s3://files-joao/SF/", "dynamic_frame")

# Transform
data_frame_00 = dynamic_frame.toDF().withColumnRenamed("col0", "trancode_mainframe")

data_frame_01 = transform_headers(data_frame_00)

data_frame_01.printSchema()
data_frame_01.show()

data_frame_01.toPandas().to_csv('mycsv.csv')

job.commit()