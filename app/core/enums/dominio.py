from enum import Enum

from app.core.processors.metadata_processor import MetadataProcessor
from app.core.processors.operacao_processor import OperacaoProcessor
from app.core.processors.originacao_processor import OriginacaoProcessor
from app.core.processors.parcela_processor import ParcelaProcessor


class Dominio(Enum):
    METADATA = (0, lambda lista_trancode, codigo_identificacao_carga: MetadataProcessor(lista_trancode, codigo_identificacao_carga))
    OPERACAO = (1, lambda lista_trancode, codigo_identificacao_carga: OperacaoProcessor(lista_trancode, codigo_identificacao_carga))
    ORIGINACAO = (2, lambda lista_trancode, codigo_identificacao_carga: OriginacaoProcessor(lista_trancode, codigo_identificacao_carga))
    PARCELA = (3, lambda lista_trancode, codigo_identificacao_carga: ParcelaProcessor(lista_trancode, codigo_identificacao_carga))

    def choose_method_process(self, lista_trancode, codigo_identificacao_carga):
        return self.value[1](lista_trancode, codigo_identificacao_carga)

    def get_instance(codigo_dominio):
        for val in Dominio:
            if val.value[0] == codigo_dominio:
                return val