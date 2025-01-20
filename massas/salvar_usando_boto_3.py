import sys
import botocore
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import json
from datetime import datetime
from awsglue import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


def salvar_dynamodb(data_frame):
    try:
        print('iniciando salvamento do dynamodb')
        session = boto3.Session()
        dynamodb = session.resource('dynamodb')
        table = dynamodb.Table('tb_operacoes')

        with table.batch_writer() as batch:
            for item in data_frame:
                item_dict = {
                    'id_operacao': item.id_operacao,
                    'dominio': item.dominio,
                    'nome': item.nome
                }
                print(f'item que vai salvar no dynamo = {item_dict}')
                batch.put_item(Item=item_dict)
    except botocore.exceptions.ClientError as err:
        print('Error Code: {}'.format(err.response['Error']['Code']))
        print('Error Message: {}'.format(err.response['Error']['Message']))
        print('Http Code: {}'.format(err.response['ResponseMetadata']['HTTPStatusCode']))
        print('Request ID: {}'.format(err.response['ResponseMetadata']['RequestId']))

        if err.response['Error']['Code'] in ('ProvisionedThroughputExceededException', 'ThrottlingException'):
            print("Received a throttle")
        elif err.response['Error']['Code'] == 'InternalServerError':
            print("Received a server error")
        else:
            print("Erro não conhecido")
            raise err


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glue_client = boto3.client("glue")
s3 = boto3.resource('s3')
client = boto3.client('s3')

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

bucket_name = 'files-joao'
path_arquivo_name = 'SF/CARGA_ONLINE/'

print(f'os argumentos são {args}')

df = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    connection_options={"paths": ['s3://files-joao/SF'], "recurse": False},
    transformation_ctx="transformation_ctx")

print('show schema')
df.printSchema()
data_frame = df.toDF().withColumnRenamed("col0", "trancode_mainframe")
print('show data frame')
data_frame.show(10)
column_base = 'trancode_mainframe'
new_data_frame = (data_frame
                  .withColumn('id_operacao', col(column_base).substr(0, 5))
                  .withColumn('dominio', col(column_base).substr(6, 10))
                  .withColumn('nome', col(column_base).substr(16, 1000)))

print('show new data frame')
new_data_frame.show(50)

new_data_frame.foreachPartition(salvar_dynamodb)
