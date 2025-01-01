from pyspark.sql.functions import col
from pyspark.sql.functions import explode
from pyspark.sql.functions import from_json
from pyspark.sql.types import StringType, ArrayType, StructType, StructField


class MovimentoFinanceiroDemocratizer():

    def __init__(self, data_frame):
        self.data_frame = data_frame

    def democratizar(self):
        schema = ArrayType(StructType([
            StructField("custodia", StringType(), True),
            StructField("id_operacao", StringType(), True),
            StructField("id_evento", StringType(), True),
            StructField("data_evento", StringType(), True),
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
            col("value.id_evento").alias("id_evento"),
            col("value.data_evento").alias("data_evento"),
            col("value.codigo_identificacao_carga").alias("codigo_identificacao_carga")
        )

        novo.printSchema()
        novo.show(truncate=False)

        # Salvar no S3 via api do Glue Context
