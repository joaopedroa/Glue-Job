from app.core.enums.origem_carga import OrigemCarga
# from app.dataprovider.dynamo_adapter import DynamoAdapter
# from app.dataprovider.s3_adapter import S3Adapter
from app.dataprovider.trancode_adapter import TrancodeAdapter
# from awsglue import DynamicFrame
# from awsglue.context import GlueContext
from datetime import datetime
import pytz

class CargaOfflineStrategy():
    def __init__(self, context_dynamo, context_s3, glue_context, trancode_adapter: TrancodeAdapter, dynamo_adapter, s3_adapter):
        self.glue_context = glue_context
        self.context_dynamo = context_dynamo
        self.context_s3 = context_s3
        self.trancode_adapter = trancode_adapter
        self.dynamo_adapter = dynamo_adapter
        self.s3_adapter = s3_adapter

    def processar(self, data_frame):
        data_frame.show()
        data_frame_01 = self.trancode_adapter.transformar_dados_trancode_header(data_frame)
        data_frame_01.show()
        data_frame_02 = self.trancode_adapter.transformar_dados_trancode_body(data_frame_01)
        data_frame_02.show()
        data_frame_02.toPandas().to_csv('mycsv.csv')
        # dynamic_frame_dynamo = DynamicFrame.fromDF(data_frame_02, self.glue_context, self.context_dynamo)
        # data_frame_02.toPandas().to_csv('mycsv.csv')
        # self.dynamo_adapter.salvar(dynamic_frame_dynamo)
        # self.__arquivar_dados_processados(data_frame)

    # def __arquivar_dados_processados(self, data_frame):
    #     dynamic_frame_para_arquivamento = DynamicFrame.fromDF(data_frame, self.glue_context, self.context_s3)
    #     particao = datetime.now(pytz.timezone("America/Sao_Paulo")).strftime("%Y-%m-%d")
    #     path = OrigemCarga.CARGA_OFFLINE.get_path_bucket_carga_arquivamento(particao)
    #     self.s3_adapter.salvar(dynamic_frame_para_arquivamento, path)
