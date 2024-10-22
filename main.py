import sys

from app.core.enums.dominio import Dominio
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, substring, lit, udf
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from datetime import date

def extract(path, transformation_ctx, glueContext):
    return (
    glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="csv",
        connection_options={"paths": [path], "recurse": False},
        transformation_ctx=transformation_ctx))

@udf(returnType=StringType())
def processar_trancode_de_dados_udf(lista_trancode, codigo_dominio, codigo_identificador_carga):
    dominio = Dominio.get_instance(codigo_dominio)
    return dominio.choose_method_process(lista_trancode, codigo_identificador_carga).processar()

@udf(returnType=StringType())
def to_dominio(codigo_dominio, custodia, codigo_identificador_carga):
    return Dominio.get_instance(codigo_dominio).name +  "#" + custodia + "#" + codigo_identificador_carga

def transform_headers(data_frame):
    column_base = 'trancode_mainframe'
    return (((data_frame
            .withColumn('custodia',  col(column_base).substr(0, 2))
            .withColumn('codigo_dominio',  col(column_base).substr(3, 2).cast("Integer"))
            .withColumn('id_operacao',  col(column_base).substr(5, 9).cast("Long"))
            .withColumn('codigo_identificador_carga',  col(column_base).substr(14, 15))
            .withColumn('dominio', to_dominio(col('codigo_dominio'), col('custodia'), col('codigo_identificador_carga')))
            .withColumn('trancode',  col(column_base).substr(29, 100000))
            ).groupby("custodia", "codigo_dominio", "dominio", "id_operacao", "codigo_identificador_carga").agg(F.collect_set("trancode").alias('lista_trancode'))
            ).withColumn('dados_dominio',  processar_trancode_de_dados_udf(col('lista_trancode'),col('codigo_dominio'), col('codigo_identificador_carga')))
            .drop("lista_trancode")
            .drop("codigo_dominio")
            .orderBy(col("id_operacao").asc(), col("codigo_dominio").asc()))

def main():
    # args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    job = Job(glueContext)
    # job.init(args['JOB_NAME'], args)

    # Extract
    dynamic_frame = extract("s3://files-joao/SF/", "dynamic_frame", glueContext)

    # Transform
    data_frame_00 = dynamic_frame.toDF().withColumnRenamed("col0", "trancode_mainframe")
    data_frame_01 = transform_headers(data_frame_00)

    data_frame_01.printSchema()
    data_frame_01.show()

    dynamo_frame_transform = DynamicFrame.fromDF(data_frame_01, glueContext, "dynamic_frame")

    #Load DynamoDB - Consulta Transacional
    glueContext.write_dynamic_frame.from_options(
        frame=dynamo_frame_transform,
        connection_type="dynamodb",
        connection_options={
            "dynamodb.output.tableName": "operacoes",
            "dynamodb.throughput.write.percent": "1.0"
        }
    )

    # Load S3 - Democratização de Dados
    particao = date.today()
    glueContext.write_dynamic_frame.from_options(
        frame=dynamo_frame_transform,
        connection_type="s3",
        connection_options= {
            "path": f's3://glue-teste-joao/{particao}'
        },
        format="csv"
    )

    job.commit()

# Start
main()



