from enum import Enum

from app.core.domains.metadata import Metadata
from app.core.domains.operacao import Operacao
from app.core.domains.originacao import Originacao
from app.core.domains.parcela import Parcela


class Dominio(Enum):
    METADATA = (0, lambda trancode, codigo_identificacao_carga: Metadata(trancode, codigo_identificacao_carga))
    OPERACAO = (1, lambda trancode, codigo_identificacao_carga: Operacao(trancode, codigo_identificacao_carga))
    ORIGINACAO = (2, lambda trancode, codigo_identificacao_carga: Originacao(trancode, codigo_identificacao_carga))
    PARCELA = (3, lambda trancode, codigo_identificacao_carga: Parcela(trancode, codigo_identificacao_carga))

    def choose_method(self, trancode, codigo_identificacao_carga):
        return self.value[1](trancode, codigo_identificacao_carga)

    def get_instance(codigo_dominio):
        for val in Dominio:
            if val.value[0] == codigo_dominio:
                return val