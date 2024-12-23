import boto3
import json

class S3Adapter:
    def __init__(self, glue_context):
        self.glue_context = glue_context
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')

    def extrair_csv(self, path, transformation_ctx):
        return (
            self.glue_context.create_dynamic_frame.from_options(
                connection_type="s3",
                format="csv",
                connection_options={"paths": [path], "recurse": False},
                transformation_ctx=transformation_ctx))

    def extrair_lista_json(self, path, transformation_ctx):
        return (
            self.glue_context.create_dynamic_frame.from_options(
                connection_type="s3",
                format="json",
                format_options={"jsonPath": "$[*]",  "multiline": True,},
                connection_options={"paths": [path], "recurse": False},
                transformation_ctx=transformation_ctx))

    def salvar(self, dynamic_frame, path):
        self.glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options={
                "path": path
            },
            format_options={
                "writeHeader": 'false',
                "quoteChar": '-1',
                "separator": '|'
            },
            format="csv"
        )


    def mover_arquivo(self, bucket, origin_path, target_path):
        copy_source = {
            'Bucket': bucket,
            'Key': origin_path
        }
        self.s3_resource.meta.client.copy(copy_source, Bucket=bucket, Key=target_path)
        self.s3_client.delete_object(Bucket=bucket, Key=origin_path)


    def buscar_nome_arquivo_temporario(self, bucket_name, prefix):
        response = self.s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix)
        return response['Contents'][0]['Key']

    def recuperar_conteudo_arquivo_temporario(self, bucket_name, path_file):
        response = self.s3_client.get_object(Bucket=bucket_name, Key=path_file)
        return json.loads(result["Body"].read().decode())[0]