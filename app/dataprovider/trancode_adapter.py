from pyspark.sql.functions import col, udf, expr, when, explode_outer
from pyspark.sql.types import StringType, BooleanType
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from app.core.dtos.dominio_dto import DominioDTO
from app.core.enums.dominio_enum import Dominio
from pyspark.sql.functions import from_json
from pyspark.sql.functions import explode
from pyspark.sql.types import MapType, StringType, ArrayType, StructType, StructField


class TrancodeAdapter:

    def __init__(self):
        pass

    def recuperar_data_frame_base(self, data_frame, spark):
        data_frame.createOrReplaceTempView("tb_base")
        return spark.sql("""
                   SELECT 
                       SUBSTRING(trancode_mainframe, 1, 2) as custodia,
                       INT(SUBSTRING(trancode_mainframe, 3, 2)) as codigo_dominio,
                       SUBSTRING(trancode_mainframe, 4, 9) as id_operacao,
                       SUBSTRING(trancode_mainframe, 14, 15) as codigo_identificador_carga,
                       CASE SUBSTRING(trancode_mainframe, 3, 2)
                           WHEN 99 THEN 'METADATA'
                           WHEN 1 THEN 'OPERACAO'
                           WHEN 2 THEN 'ORIGINACAO'
                           WHEN 3 THEN 'PARCELA'
                           WHEN 100 THEN 'PARCELA_CONTROLE_IOF'
                           WHEN 4 THEN 'MOVIMENTO_FINANCEIRO'
                           ELSE 'NAO_ENCONTRADO'
                       END as dominio_enum,
                       CASE SUBSTRING(trancode_mainframe, 3, 2)
                           WHEN 99 THEN CONCAT('METADATA', '#', SUBSTRING(trancode_mainframe, 1, 2), '#', SUBSTRING(trancode_mainframe, 14, 15))
                           WHEN 1 THEN CONCAT('OPERACAO', '#', SUBSTRING(trancode_mainframe, 1, 2), '#', SUBSTRING(trancode_mainframe, 14, 15))
                           WHEN 2 THEN CONCAT('ORIGINACAO', '#', SUBSTRING(trancode_mainframe, 1, 2), '#', SUBSTRING(trancode_mainframe, 14, 15))
                           WHEN 3 THEN CONCAT('PARCELA', '#', SUBSTRING(trancode_mainframe, 1, 2), '#', SUBSTRING(trancode_mainframe, 14, 15))
                           WHEN 100 THEN CONCAT('PARCELA_CONTROLE_IOF', '#', SUBSTRING(trancode_mainframe, 1, 2), '#', SUBSTRING(trancode_mainframe, 14, 15))
                           WHEN 4 THEN CONCAT('MOVIMENTO_FINANCEIRO', '#', SUBSTRING(trancode_mainframe, 1, 2), '#', SUBSTRING(trancode_mainframe, 14, 15))
                           ELSE CONCAT('NAO_ENCONTRADO', '#', SUBSTRING(trancode_mainframe, 1, 2), '#', SUBSTRING(trancode_mainframe, 14, 15))
                       END as dominio,
                       SUBSTRING(trancode_mainframe, 29) as trancode
                   FROM tb_base
               """)
    def transformar_dados_trancode_header_operacoes(self, data_frame, spark):
        data_frame.createOrReplaceTempView("tb_operacoes")
        return spark.sql("""
                    SELECT 
                        tb_operacoes.*,
                        CASE codigo_dominio
                           WHEN 1 THEN SUBSTRING(trancode, 12, 10) 
                           ELSE null
                       END as data_contratacao,
                       CASE codigo_dominio
                           WHEN 3 THEN SUBSTRING(trancode, 12, 2) 
                           ELSE null
                       END as numero_parcela,
                        CASE codigo_dominio
                           WHEN 3 THEN SUBSTRING(trancode, 12, 2) 
                           ELSE null
                       END as numero_plano,
                        CASE codigo_dominio
                           WHEN 3 THEN SUBSTRING(trancode, 14, 10) 
                           ELSE null
                       END as data_vencimento_parcela,
                        CASE codigo_dominio
                           WHEN 2 THEN SUBSTRING(trancode, 12, 12) 
                           ELSE null
                       END as meio_recebimento_valor,
                        CASE codigo_dominio
                           WHEN 4 THEN SUBSTRING(trancode, 12, 3) 
                           ELSE null
                       END as id_evento,
                        CASE codigo_dominio
                           WHEN 4 THEN SUBSTRING(trancode, 15, 10) 
                           ELSE null
                       END as data_evento
                    FROM tb_operacoes
                """)

    def transformar_dados_trancode_header(self, data_frame):
        column_base = 'trancode_mainframe'

        to_dominio_udf = udf(
            lambda codigo_dominio, custodia, codigo_identificador_carga: self.__to_dominio(codigo_dominio, custodia,
                                                                                           codigo_identificador_carga),
            StringType())

        to_dominio_enum_udf = udf(lambda codigo_dominio: str(Dominio.get_instance(codigo_dominio).name), StringType())

        column_list = ["id_operacao", "codigo_identificador_carga"]
        window_spec = Window.partitionBy(*column_list)

        column_list2 = ["custodia", "codigo_dominio", "dominio", "dominio_enum", "id_operacao",
                        "codigo_identificador_carga"]
        window_spec2 = Window.partitionBy(*column_list2)

        processar_todos_dados_dominios = udf(
            lambda dominio_enum, lista_trancode: self.__to_build_todos_dados_dominios(dominio_enum, lista_trancode),
            StringType())

        processar_trancode_de_dados_udf = udf(lambda trancode, codigo_dominio, codigo_identificador_carga,
                                                     dados_todos_dominios: self.__processar_trancode_de_dados(
            trancode, codigo_dominio, codigo_identificador_carga, dados_todos_dominios), StringType())

        schema = ArrayType(StructType([
            StructField("is_controle_iof", BooleanType(), True)
        ]))

        to_parcela_controle_iof = udf(
            lambda dados_dominio, dominio_enum: self.__to_parcela_controle_iof(dados_dominio, dominio_enum),
            StringType())

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
                 .withColumn('lista_trancode', F.collect_set("trancode").over(window_spec2))
            #      .withColumn('dados_todos_dominios', F.collect_set(
            # processar_todos_dados_dominios(col('dominio_enum'), col('lista_trancode'))).over(window_spec))
            #      .withColumn('dados_dominio',
            #                  processar_trancode_de_dados_udf(col('trancode'), col('codigo_dominio'),
            #                                                  col('codigo_identificador_carga'),
            #                                                  col('dados_todos_dominios')))
            #      .withColumn('teste', explode_outer(
            # from_json(to_parcela_controle_iof(col('dados_dominio'), col('dominio_enum')), schema)))
            #      .withColumn('codigo_dominio',
            #                  when(col('teste.is_controle_iof'), 100).otherwise(col('codigo_dominio')))
            #      .withColumn('dominio_enum',
            #                  when(col('teste.is_controle_iof'), Dominio.PARCELA_CONTROLE_IOF.name).otherwise(
            #                      col('dominio_enum')))
            #      .withColumn('dominio',
            #                  when(col('teste.is_controle_iof'), to_dominio_udf(col('codigo_dominio'), col('custodia'),
            #                                                                    col('codigo_identificador_carga'))).otherwise(
            #                      col('dominio')))
                 ))

    def transformar_dados_trancode_body(self, data_frame):
        organizar_trancode_de_dados_udf = udf(lambda codigo_dominio, dados_dominio: self.__organizar_trancode_de_dados(
            codigo_dominio, dados_dominio), StringType())

        return ((data_frame
        .groupby("custodia", "codigo_dominio", "dominio", "dominio_enum", "id_operacao",
                 "codigo_identificador_carga").agg(
            F.collect_set("dados_dominio").alias('lista_dados_dominio'))
                )
                .withColumn('dados_dominio',
                            organizar_trancode_de_dados_udf(col('codigo_dominio'), col('lista_dados_dominio')))
                .drop("lista_dados_dominio")
                .drop("dados_todos_dominios")
                .orderBy(col("id_operacao").asc(), col("codigo_dominio").asc())
                )

    def __to_parcela_controle_iof(self, dados_dominio, dominio_enum):
        if (dominio_enum == 'PARCELA'):
            return "[{'is_controle_iof': false}, {'is_controle_iof': true}]"
        return []

    def __to_dominio(self, codigo_dominio, custodia, codigo_identificador_carga):
        return Dominio.get_instance(codigo_dominio).name + "#" + custodia + "#" + codigo_identificador_carga

    def __to_build_todos_dados_dominios(self, dominio_enum, lista_trancode):
        return DominioDTO(dominio_enum, lista_trancode).to_json()

    def __processar_trancode_de_dados(self, lista_trancode, codigo_dominio, codigo_identificador_carga,
                                      dados_todos_dominios):
        dominio = Dominio.get_instance(codigo_dominio)
        return dominio.choose_method_process(lista_trancode, codigo_identificador_carga,
                                             dados_todos_dominios).processar()

    def __organizar_trancode_de_dados(self, codigo_dominio, dados_dominio):
        dominio = Dominio.get_instance(codigo_dominio)
        return dominio.choose_method_organizer(dados_dominio).organizar()
