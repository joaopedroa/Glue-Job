import json

from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, MapType, StructType, StructField, ArrayType

from pyspark.sql.functions import from_json
import pyspark.sql.functions as f
from pyspark.sql.functions import explode

class MetadataDemocratizer():

    def __init__(self, data_frame):
        self.data_frame = data_frame

    def democratizar(self):
        alterar = udf(lambda dados_dominio, custodia: self.__alterar_dados(dados_dominio, custodia), StringType())

        # df = (self.data_frame.select(
        #     col("custodia"),
        #     col("dominio"),
        #     col("dominio_enum"),
        #     col("codigo_dominio"),
        #     col('id_operacao'),
        #     col('codigo_identificador_carga'),
        #     F.json_tuple(col("dados_dominio"), *self.__get_fields())
        # ).toDF(*self.__get_keys(), *self.__get_fields()))

        schema = StructType([
            StructField("codigo_identificacao_carga", StringType(), True),
            StructField("quantidade_parcelas", StringType(), True),
            StructField("quantidade_movimentos_financeiros", StringType(), True),
            StructField("quantidade_parcelas_recebidas", StringType(), True),
            StructField("lista", ArrayType(StructType([
                StructField("nome", StringType(), True),
                StructField("idade", StringType(), True),
            ])), True),
        ])

        df = (self.data_frame
              .withColumn("dados_alterados",
                          from_json(self.data_frame.dados_dominio, schema)))

        df.printSchema()
        df.show(10, truncate=False)

        df_new = df.withColumn('novos_dados', F.transform('dados_alterados.lista',
                                                          lambda x: x
                                                          .withField('codigo_identificacao_carga', col('dados_alterados.codigo_identificacao_carga'))
                                                          .withField('quantidade_parcelas', col('dados_alterados.quantidade_parcelas'))
                                                          .withField('quantidade_movimentos_financeiros', col('dados_alterados.quantidade_movimentos_financeiros'))
                                                          .withField('quantidade_parcelas_recebidas', col('dados_alterados.quantidade_parcelas_recebidas'))
                                                          ))

        df_new.printSchema()
        df_new.show(10, truncate=False)

        df_new_now = df_new.withColumn('value', explode(df_new.novos_dados))
        novo = df_new_now.select(
            col("custodia").alias("custodia_core"),
            col("codigo_dominio").alias("codigo_dominio"),
            col("dominio").alias("dominio"),
            col("dominio_enum").alias("dominio_enum"),
            col("id_operacao").alias("id_operacao_core"),
            col("codigo_identificador_carga").alias("codigo_identificador_carga"),
            col("value.nome").alias("nome"),
            col("value.idade").alias("idade"),
            col("value.codigo_identificacao_carga").alias("codigo_identificacao_carga_2"),
            col("value.quantidade_parcelas").alias("quantidade_parcelas"),
            col("value.quantidade_movimentos_financeiros").alias("quantidade_movimentos_financeiros"),
            col("value.quantidade_parcelas_recebidas").alias("quantidade_parcelas_recebidas"),
        )

        novo.printSchema()
        novo.show(10, truncate=False)

        print("finalizou")

        # Salvar no S3 via api do Glue Context

    def __alterar_dados(self, dados_dominio, custodia):
        dados = json.loads(dados_dominio)
        novos_dados = list(map(lambda item: self.processar_item(item, custodia), dados['lista']))
        return novos_dados

    def processar_item(self, item, custodia):
        item['custodia'] = custodia
        return item

    def __get_fields(self):
        return (
            "codigo_identificacao_carga",
            "quantidade_parcelas",
            "quantidade_movimentos_financeiros",
            "quantidade_parcelas_recebidas"
        )

    def __get_keys(self):
        return "custodia", "dominio", "dominio_enum", "codigo_dominio", "id_operacao", "codigo_identificador_carga"
