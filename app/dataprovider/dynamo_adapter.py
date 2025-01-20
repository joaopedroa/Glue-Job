import boto3
import botocore


class DynamoAdapter:
    def __init__(self, glue_context):
        self.glue_context = glue_context

    def salvar(self, dynamic_frame):
        self.glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="dynamodb",
            connection_options={
                "dynamodb.output.tableName": "operacoes",
                "dynamodb.throughput.write.percent": "1.0"
            }
        )

    def salvar_using_boto3(self, data_frame):
        data_frame.foreachPartition(self.salvar_dynamodb)

    def salvar_dynamodb(self, data_frame):
        try:
            session = boto3.Session()
            dynamodb = session.resource('dynamodb')
            table = dynamodb.Table('tb_operacoes')

            with table.batch_writer() as batch:
                for item in data_frame:
                    batch.put_item(Item=item)
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
