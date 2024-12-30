import sys
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

df = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    connection_options={"paths": ['s3://files-joao/SF'], "recurse": False},
    transformation_ctx="transformation_ctx")
job.commit()

job_name = args['JOB_NAME']
response = glue_client.get_job(JobName=job_name)
temp_dir = response['Job']['DefaultArguments']['--TempDir']
job_run_id = args['JOB_RUN_ID']
path = f"{temp_dir}partitionlisting/{job_name}/{job_run_id}/"
print(f"path is: {path}")

bucket_name = temp_dir.split('/')[2]
path_arquivo_array = path.split('/')[3:]
path_arquivo_name = '/'.join(path_arquivo_array)

print(f"Bucket name is {bucket_name}")
print(f"Path name is {path_arquivo_name}")

dynamicFrame = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    connection_options={"paths": ['s3://files-joao/SF/'], "recurse": False},
    transformation_ctx="transformation_ctx111")

data_frame = dynamicFrame.toDF()

data_frame.show(2)
print("printouuu")

to_data_udf = udf(lambda: datetime.today().strftime('%Y-%m-%d'), StringType())

column_base = 'col0'

data_frame2 = data_frame.withColumn('custodia', col(column_base).substr(0, 2)).withColumn('id_carga',
                                                                                          col(column_base).substr(3,
                                                                                                                  2).cast(
                                                                                              "Integer")).withColumn(
    'id_operacao', col(column_base).substr(5, 9).cast("Long")).withColumn('data_versao', to_data_udf())

data_frame2.show(10)

dynamicFrame = DynamicFrame.fromDF(data_frame2, glue_context, "transformation_ctx_2")

dynamicFrame = ApplyMapping.apply(frame=dynamicFrame, mappings=[("custodia", "string", "custodia", "string"),
                                                                ("id_operacao", "Long", "id_operacao", "bigint"),
                                                                ("id_carga", "Integer", "id_carga", "string"),
                                                                ("data_versao", "string", "data_versao", "string")
                                                                ], transformation_ctx="transformation_ctx111")

glue_context.write_dynamic_frame_from_catalog(
    frame=dynamicFrame,
    database="pos-venda",
    table_name='tb_operacoes',
    transformation_ctx="write_sink",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "LOG",
        "partitionKeys": ["data_versao", "id_carga"],
        "compression": "snappy"
    }
)

print('democratizouuu')

df2 = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    format_options={"jsonPath": "$[*]", "multiline": False},
    connection_options={"paths": [path], "recurse": False},
    transformation_ctx="transformation_ctx_2")
print(f"Extraiu dados de bucket temporario")
df2.printSchema()
df2.show(10)
df_rdd = df2.toDF().rdd
print(df_rdd.collect())
print('passou aqui')

response = client.list_objects_v2(
    Bucket=bucket_name,
    Prefix=path_arquivo_name)

print(response)

nome_arquivo_json = response['Contents'][0]['Key']

print(f"nome arquivo json é {nome_arquivo_json}")

response_2 = client.get_object(Bucket=bucket_name, Key=nome_arquivo_json)
print(response_2)
print('------------------')
resposta = json.loads(response_2["Body"].read().decode())
print(resposta)

for path_arquivo in resposta[0]['files']:
    bucket_name_arquivo = path_arquivo.split('/')[2]
    print(f'nome do bucket arquivo é: {bucket_name_arquivo}')

    path_sem_inicio = path_arquivo.split('/')[3:]

    tamanho_arquivo = len(path_sem_inicio)

    nome_arquivo = path_sem_inicio[tamanho_arquivo - 1:][0]
    print(f'nome do arquivo é: {nome_arquivo}')

    caminho_arquivo_destino = '/'.join(path_sem_inicio[:-1]) + '/PROCESSADOS/' + nome_arquivo
    print(f'caminho do arquivo destino é: {caminho_arquivo_destino}')

    caminho_arquivo_origem = '/'.join(path_sem_inicio)
    print(f'caminho do arquivo origem é: {caminho_arquivo_destino}')

    copy_source = {
        'Bucket': bucket_name_arquivo,
        'Key': caminho_arquivo_origem
    }

    s3.meta.client.copy(copy_source, Bucket=bucket_name_arquivo, Key=caminho_arquivo_destino)
    client.delete_object(Bucket=bucket_name_arquivo, Key=caminho_arquivo_origem)




