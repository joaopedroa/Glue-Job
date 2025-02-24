from app.core.domains.movimento_financeiro import MovimentoFinanceiro
from app.core.domains.operacao import Operacao
from app.core.domains.originacao import Originacao
from app.core.enums.dominio_enum import Dominio
from app.core.enums.origem_carga import OrigemCarga
from app.core.strategies.democratizacao.metadata_democratizer import MetadataDemocratizer
from app.core.strategies.democratizacao.movimento_financeiro_democratizer import MovimentoFinanceiroDemocratizer
from app.core.strategies.democratizacao.operacao_democratizer import OperacaoDemocratizer
from app.core.strategies.democratizacao.originacao_democratizer import OriginacaoDemocratizer
from app.core.strategies.democratizacao.parcela_democratizer import ParcelaDemocratizer
# from app.dataprovider.dynamo_adapter import DynamoAdapter
# from app.dataprovider.s3_adapter import S3Adapter
from app.dataprovider.trancode_adapter import TrancodeAdapter
import time
# from awsglue import DynamicFrame
# from awsglue.context import GlueContext
from datetime import datetime
import pytz
from pyspark.sql.functions import explode
from pyspark.sql.functions import col, udf

class CargaOfflineStrategy():
    def __init__(self, context_dynamo, context_s3, glue_context, trancode_adapter: TrancodeAdapter, dynamo_adapter, s3_adapter):
        self.glue_context = glue_context
        self.context_dynamo = context_dynamo
        self.context_s3 = context_s3
        self.trancode_adapter = trancode_adapter
        self.dynamo_adapter = dynamo_adapter
        self.s3_adapter = s3_adapter

    def processar(self, data_frame, spark):
        data_frame.show()
        start_time = time.time()
        data_frame_base = self.trancode_adapter.recuperar_data_frame_base(data_frame, spark)
        data_frame_base.show()
        data_frame_01 = self.trancode_adapter.transformar_dados_trancode_header_operacoes(data_frame_base, spark)
        data_frame_01.show(100)


        print("--- %s seconds ---" % (time.time() - start_time))
        # data_frame_01.show()
        # data_frame_02 = self.trancode_adapter.transformar_dados_trancode_body(data_frame_01)
        # data_frame_02.show(truncate=False)
        # data_frame_02.toPandas().to_csv('mycsv.csv')
        # data_frame_02.repartition(100)
        # print(f'numero de particoes = {data_frame.rdd.getNumPartitions()}')
        #
        # data_frame_democratizacao = data_frame_02.filter(data_frame_02.codigo_dominio == Dominio.METADATA.value[0])
       # MetadataDemocratizer(data_frame_democratizacao).democratizar()
        # Dominio.METADATA.choose_method_democratizer(data_frame_democratizacao).democratizar()

        # for dominio in Dominio:
        #     print(f'Código do domínio é {dominio.value[0]}')
        #     data_frame_democratizacao = data_frame_02.filter(data_frame_02.codigo_dominio == dominio.value[0])
        #     dominio.choose_method_democratizer(data_frame_democratizacao).democratizar()


        # dynamic_frame_dynamo = DynamicFrame.fromDF(data_frame_02, self.glue_context, self.context_dynamo)
        # data_frame_02.toPandas().to_csv('mycsv.csv')
        # self.dynamo_adapter.salvar(dynamic_frame_dynamo)
        # self.__arquivar_dados_processados(data_frame)

    # def __arquivar_dados_processados(self, data_frame):
    #     dynamic_frame_para_arquivamento = DynamicFrame.fromDF(data_frame, self.glue_context, self.context_s3)
    #     particao = datetime.now(pytz.timezone("America/Sao_Paulo")).strftime("%Y-%m-%d")
    #     path = OrigemCarga.CARGA_OFFLINE.get_path_bucket_carga_arquivamento(particao)
    #     self.s3_adapter.salvar(dynamic_frame_para_arquivamento, path)
