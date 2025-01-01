from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import MapType,StringType,ArrayType,StructType,StructField
from pyspark.sql.functions import from_json
from pyspark.sql.functions import explode
class ParcelaDemocratizer():

    def __init__(self, data_frame):
        self.data_frame = data_frame

    def democratizar(self):
        schema = ArrayType(StructType([
            StructField("custodia", StringType(), True),
            StructField("id_operacao", StringType(), True),
            StructField("numero_plano", StringType(), True),
            StructField("numero_parcela", StringType(), True),
            StructField("data_vencimento_parcela", StringType(), True),
            StructField("codigo_identificacao_carga", StringType(), True),
                                       ]))

        df = (self.data_frame
              .withColumn("value", explode(from_json(self.data_frame.dados_dominio, schema)))
              .drop("dados_dominio")
              )

        novo = df.select(
            col("custodia").alias("custodia_core"),
            col("codigo_dominio").alias("codigo_dominio"),
            col("dominio").alias("dominio"),
            col("dominio_enum").alias("dominio_enum"),
            col("id_operacao").alias("id_operacao_core"),
            col("codigo_identificador_carga").alias("codigo_identificador_carga"),
            col("value.custodia").alias("custodia"),
            col("value.id_operacao").alias("id_operacao"),
            col("value.numero_plano").alias("numero_plano"),
            col("value.numero_parcela").alias("numero_parcela"),
            col("value.data_vencimento_parcela").alias("data_vencimento_parcela"),
            col("value.codigo_identificacao_carga").alias("id_carga")
        )

        novo.printSchema()
        novo.show(truncate=False)

        # Salvar no S3 via api do Glue Context
