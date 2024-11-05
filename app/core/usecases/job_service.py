from app.config.logger import Logger
from app.core.enums.origem_carga import OrigemCarga
from app.core.strategies.carga_offline_strategy import CargaOfflineStrategy
from app.core.strategies.carga_online_strategy import CargaOnlineStrategy
from app.dataprovider.dynamo_adapter import DynamoAdapter
from app.dataprovider.s3_adapter import S3Adapter
from app.dataprovider.trancode_adapter import TrancodeAdapter
from app.dataprovider.aws_adapter import AwsAdapter
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.job import Job
import sys

class JobService:
    def __init__(self):
        # self.args_glue = getResolvedOptions(sys.argv, ['JOB_NAME'])
        self.spark_context = SparkContext()
        self.glue_context = GlueContext(self.spark_context)
        # self.aws_adapter = AwsAdapter(self.args_glue)
        self.aws_adapter = AwsAdapter(None)
        self.s3_adapter = S3Adapter(self.glue_context)
        self.dynamo_adapter = DynamoAdapter(self.glue_context)
        self.trancode_adapter = TrancodeAdapter()
        self.spark = self.glue_context.spark_session
        self.job = Job(self.glue_context)
        # self.job_name = self.args_glue['JOB_NAME']
        # self.workflow_name = self.args_glue['WORKFLOW_NAME']
        # self.workflow_run_id =self.args_glue['WORKFLOW_RUN_ID']
        self.logger = Logger("self.job_name", "self.workflow_name", "self.workflow_run_id")
        self.context_dynamo = "carga_dynamo_context"
        self.context_s3 = "carga_s3_context"
        self.carga_strategy_map = {
            OrigemCarga.CARGA_ONLINE: CargaOnlineStrategy(self.context_dynamo, self.context_s3, self.glue_context, self.trancode_adapter, self.dynamo_adapter, self.s3_adapter),
            OrigemCarga.CARGA_OFFLINE: CargaOfflineStrategy(self.context_dynamo, self.context_s3, self.glue_context, self.trancode_adapter, self.dynamo_adapter, self.s3_adapter),
        }

    def processar_job(self):
        self.logger.log("Iniciando processamento")
        job2 = Job(self.glue_context)
        # self.job.init(self.args_glue['JOB_NAME'], self.args_glue)

        # origem_carga = self.aws_adapter.recuperar_origem_carga_workflow()
        origem_carga = OrigemCarga.CARGA_OFFLINE

        # self.logger.log(f"Origem da carga para processamento: {origem_carga.get_descricao_carga()}")

        path_s3_source = origem_carga.get_path_bucket_carga()

        dynamic_frame = self.s3_adapter.extrair_csv(path_s3_source, "dynamic_frame_context")
        self.job.commit()

        data_frame = dynamic_frame.toDF()
        if data_frame.isEmpty():
            self.logger.log(f"Não possui dados para processamento")
        else:
            self.logger.log(f"Iniciando processamento de estrategia")
            data_frame = data_frame.withColumnRenamed("col0", "trancode_mainframe")
            self.carga_strategy_map[origem_carga].processar(data_frame)
            self.logger.log(f"Processamento de estrategia finalizada com sucesso")

            # arquivos_processados = self.aws_adapter.recuperar_path_arquivos_processados()

        self.job.commit()








