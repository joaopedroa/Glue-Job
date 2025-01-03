from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from app.core.dtos.dominio_dto import DominioDTO
from app.core.enums.dominio_enum import Dominio

class TrancodeAdapter:

    def __init__(self):
        pass

    def transformar_dados_trancode_header(self, data_frame):
        column_base = 'trancode_mainframe'

        to_dominio_udf = udf(lambda codigo_dominio, custodia, codigo_identificador_carga: self.__to_dominio(codigo_dominio, custodia, codigo_identificador_carga), StringType())
        to_dominio_enum_udf = udf(lambda codigo_dominio: str(Dominio.get_instance(codigo_dominio).name), StringType())

        return ((data_frame
                 .withColumn('custodia', col(column_base).substr(0, 2))
                 .withColumn('codigo_dominio', col(column_base).substr(3, 2).cast("Integer"))
                 .withColumn('id_operacao', col(column_base).substr(5, 9).cast("Long"))
                 .withColumn('codigo_identificador_carga', col(column_base).substr(14, 15))
                 .withColumn('dominio_enum', to_dominio_enum_udf(col('codigo_dominio')))
                 .withColumn('dominio',
                             to_dominio_udf(col('codigo_dominio'), col('custodia'),
                                               col('codigo_identificador_carga')))
                 .withColumn('trancode', col(column_base).substr(29, 100000))
                 ))


    def transformar_dados_trancode_body(self, data_frame):

        processar_trancode_de_dados_udf = udf(lambda lista_trancode, codigo_dominio, codigo_identificador_carga, dados_todos_dominios: self.__processar_trancode_de_dados(lista_trancode, codigo_dominio, codigo_identificador_carga, dados_todos_dominios), StringType())

        processar_todos_dados_dominios = udf(lambda dominio_enum, lista_trancode: self.__to_build_todos_dados_dominios(dominio_enum, lista_trancode), StringType())

        column_list = ["id_operacao", "codigo_identificador_carga"]
        window_spec = Window.partitionBy(*column_list)

        return ((data_frame
         .groupby("custodia", "codigo_dominio", "dominio", "dominio_enum", "id_operacao", "codigo_identificador_carga").agg(
            F.collect_set("trancode").alias('lista_trancode'))
                )
                .withColumn('dados_todos_dominios', F.collect_set(processar_todos_dados_dominios(col('dominio_enum'), col('lista_trancode'))).over(window_spec))
                .withColumn('dados_dominio',
                            processar_trancode_de_dados_udf(col('lista_trancode'), col('codigo_dominio'),
                                                                   col('codigo_identificador_carga'),  col('dados_todos_dominios')  ))
                .drop("lista_trancode")
                .drop("codigo_dominio")
                .drop("dados_todos_dominios")
                .orderBy(col("id_operacao").asc(), col("codigo_dominio").asc())
                )


    def __to_dominio(self, codigo_dominio, custodia, codigo_identificador_carga):
        return Dominio.get_instance(codigo_dominio).name + "#" + custodia + "#" + codigo_identificador_carga

    def __to_build_todos_dados_dominios(self, dominio_enum, lista_trancode):
        return DominioDTO(dominio_enum, lista_trancode).to_json()

    def __processar_trancode_de_dados(self, lista_trancode, codigo_dominio, codigo_identificador_carga, dados_todos_dominios):
        dominio = Dominio.get_instance(codigo_dominio)
        return dominio.choose_method_process(lista_trancode, codigo_identificador_carga, dados_todos_dominios).processar()
