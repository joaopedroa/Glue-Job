from pyspark.sql import functions as F
from pyspark.sql.functions import col


class MetadataDemocratizer():

    def __init__(self, data_frame):
        self.data_frame = data_frame

    def democratizar(self):
        df = (self.data_frame.select(
            col("custodia"),
            col("dominio"),
            col("dominio_enum"),
            col("codigo_dominio"),
            col('id_operacao'),
            col('codigo_identificador_carga'),
            F.json_tuple(col("dados_dominio"), *self.__get_fields())
        ).toDF(*self.__get_keys(), *self.__get_fields()))

        df.printSchema()
        df.show()

        # Salvar no S3 via api do Glue Context

    def __get_fields(self):
        return (
            "codigo_identificacao_carga",
            "quantidade_parcelas",
            "quantidade_movimentos_financeiros",
            "quantidade_parcelas_recebidas"
        )

    def __get_keys(self):
        return "custodia", "dominio", "dominio_enum", "codigo_dominio", "id_operacao", "codigo_identificador_carga"
