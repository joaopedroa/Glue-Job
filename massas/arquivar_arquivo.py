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

bucket_name = 'files-joao'
path_arquivo_name = 'SF/CARGA_ONLINE/'

print(f'os argumentos são {args}')

response = client.list_objects_v2(
    Bucket=bucket_name,
    Prefix=path_arquivo_name,
    StartAfter=path_arquivo_name)

bucket = 'files-joao'
lista_arquivos_para_arquivar = []
if 'Contents' in response:
    path_a_processar = None
    for key in response['Contents']:
        chave = key['Key']
        chaves = chave.split('/')
        print(f'chaves = {chaves}')

        custodia = chaves[0]
        print(f'custodia = {custodia}')

        nome_arquivo = chaves[len(chaves) - 1]
        print(f'nome do arquivo = {nome_arquivo}')

        tipo_carga = chaves[1]
        print(f'Tipo da carga = {tipo_carga}')

        job_run_id = args['JOB_RUN_ID']

        caminho_arquivo_destino = f'{custodia}/CONTROLE/{tipo_carga}/A_PROCESSAR/{job_run_id}/{nome_arquivo}'
        lista_arquivos_para_arquivar.append(caminho_arquivo_destino)
        print(f'nome do arquivo de destino = {caminho_arquivo_destino}')

        copy_source = {
            'Bucket': bucket,
            'Key': chave
        }

        s3.meta.client.copy(copy_source, Bucket=bucket, Key=caminho_arquivo_destino)
        client.delete_object(Bucket=bucket, Key=chave)
        path_a_processar = 's3://files-joao/' + '/'.join(caminho_arquivo_destino.split('/')[:-1])


    print(f'path a processar {path_a_processar}')
    print(f'arquivos para arquivar {lista_arquivos_para_arquivar}')

    dynamic_frame = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        format="csv",
        connection_options={"paths": [path_a_processar], "recurse": False},
        transformation_ctx="transformation_ctx111")

    data_frame = dynamic_frame.toDF()
    print('showwww')
    data_frame.show(2)

    for chave in lista_arquivos_para_arquivar:
        chaves = chave.split('/')
        print(f'chaves 2 = {chaves}')

        custodia = chaves[0]
        print(f'custodia 2 = {custodia}')

        nome_arquivo = chaves[len(chaves) - 1]
        print(f'nome do arquivo 2 = {nome_arquivo}')

        tipo_carga = chaves[2]
        print(f'Tipo da carga 2 = {tipo_carga}')

        job_run_id = args['JOB_RUN_ID']

        caminho_arquivo_destino = f'{custodia}/CONTROLE/{tipo_carga}/PROCESSADOS/{job_run_id}/{nome_arquivo}'
        print(f'nome do arquivo de destino = {caminho_arquivo_destino}')

        copy_source = {
            'Bucket': bucket,
            'Key': chave
        }

        s3.meta.client.copy(copy_source, Bucket=bucket, Key=caminho_arquivo_destino)
        client.delete_object(Bucket=bucket, Key=chave)



