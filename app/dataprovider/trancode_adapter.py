from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from app.core.enums.dominio import Dominio

class TrancodeAdapter:

    def __init__(self):
        pass

    def transformar_dados_trancode_header(self, data_frame):
        column_base = 'trancode_mainframe'
        return ((data_frame
                 .withColumn('custodia', col(column_base).substr(0, 2))
                 .withColumn('codigo_dominio', col(column_base).substr(3, 2).cast("Integer"))
                 .withColumn('id_operacao', col(column_base).substr(5, 9).cast("Long"))
                 .withColumn('codigo_identificador_carga', col(column_base).substr(14, 15))
                 .withColumn('dominio',
                             self.__to_dominio(col('codigo_dominio'), col('custodia'),
                                               col('codigo_identificador_carga')))
                 .withColumn('trancode', col(column_base).substr(29, 100000))
                 ))

    def transformar_dados_trancode_body(self, data_frame):
        return ((data_frame
        .groupby("custodia", "codigo_dominio", "dominio", "id_operacao", "codigo_identificador_carga").agg(
            F.collect_set("trancode").alias('lista_trancode'))
                )
                .withColumn('dados_dominio',
                            self.__processar_trancode_de_dados_udf(col('lista_trancode'), col('codigo_dominio'),
                                                                   col('codigo_identificador_carga')))
                .drop("lista_trancode")
                .drop("codigo_dominio")
                .orderBy(col("id_operacao").asc(), col("codigo_dominio").asc())
                )

    @udf(returnType=StringType())
    def __to_dominio(self, codigo_dominio, custodia, codigo_identificador_carga):
        return Dominio.get_instance(codigo_dominio).name + "#" + custodia + "#" + codigo_identificador_carga

    @udf(returnType=StringType())
    def __processar_trancode_de_dados_udf(self, lista_trancode, codigo_dominio, codigo_identificador_carga):
        dominio = Dominio.get_instance(codigo_dominio)
        return dominio.choose_method_process(lista_trancode, codigo_identificador_carga).processar()
