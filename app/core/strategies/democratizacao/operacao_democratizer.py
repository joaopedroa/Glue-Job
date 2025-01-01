from pyspark.sql import functions as F
from pyspark.sql.functions import col


class OperacaoDemocratizer():

    def __init__(self, data_frame):
        self.data_frame = data_frame

    def democratizar(self):
        df = (self.data_frame.select(
            col("custodia").alias("custodia_core"),
            col("dominio"),
            col("dominio_enum"),
            col("codigo_dominio"),
            col('id_operacao').alias("id_operacao_core"),
            col('codigo_identificador_carga').alias("id_carga"),
            F.json_tuple(col("dados_dominio"), *self.__get_fields())
        ).toDF(*self.__get_keys(), *self.__get_fields()))

        df.printSchema()
        df.show()

        # Salvar no S3 via api do Glue Context

    def __get_fields(self):
        return (
            "custodia",
            "id_operacao",
            "data_contratacao",
            "codigo_identificacao_carga"
        )

    def __get_keys(self):
        return "custodia_core", "dominio", "dominio_enum", "codigo_dominio", "id_operacao_core", "id_carga"
