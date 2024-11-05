from enum import Enum


class OrigemCarga(Enum):
    CARGA_ONLINE = ("Carga de 10 em 10 minutos (Online)", "s3://files-joao/SF/CARGA_ONLINE")
    CARGA_OFFLINE = ("Carga única diária (Batch)", "s3://files-joao/SF")

    def get_descricao_carga(self):
        return self.value[0]

    def get_path_bucket_carga(self):
        return self.value[1]

    def get_path_bucket_carga_arquivamento(self, chave_particao):
        return f"{self.get_path_bucket_carga()}/PROCESSADOS/{chave_particao}"
