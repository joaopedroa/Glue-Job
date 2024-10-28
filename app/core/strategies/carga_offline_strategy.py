from app.dataprovider.dynamo_adapter import DynamoAdapter
from app.dataprovider.s3_adapter import S3Adapter
from app.dataprovider.trancode_adapter import TrancodeAdapter
from awsglue import DynamicFrame
from awsglue.context import GlueContext


class CargaOfflineStrategy():
    def __init__(self, context_dynamo, context_s3, glue_context: GlueContext, trancode_adapter: TrancodeAdapter, dynamo_adapter: DynamoAdapter, s3_adapter: S3Adapter):
        self.glue_context = glue_context
        self.context_dynamo = context_dynamo
        self.context_s3 = context_s3
        self.trancode_adapter = trancode_adapter
        self.dynamo_adapter = dynamo_adapter
        self.s3_adapter = s3_adapter

    def processar(self, data_frame):
        data_frame_01 = self.trancode_adapter.transformar_dados_trancode_header(data_frame)
        data_frame_02 = self.trancode_adapter.transformar_dados_trancode_body(data_frame_01)

        dynamic_frame_dynamo = DynamicFrame.fromDF(data_frame_02, self.glue_context, self.context_dynamo)

        self.dynamo_adapter.salvar(dynamic_frame_dynamo)