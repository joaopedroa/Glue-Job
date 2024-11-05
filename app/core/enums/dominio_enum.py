from enum import Enum

from app.core.processors.metadata_processor import MetadataProcessor
from app.core.processors.movimento_financeiro_processor import MovimentoFinanceiroProcessor
from app.core.processors.operacao_processor import OperacaoProcessor
from app.core.processors.originacao_processor import OriginacaoProcessor
from app.core.processors.parcela_processor import ParcelaProcessor


class Dominio(Enum):
    METADATA = (99, lambda lista_trancode, codigo_identificacao_carga, dados_todos_dominios: MetadataProcessor(lista_trancode, codigo_identificacao_carga, dados_todos_dominios))
    OPERACAO = (1, lambda lista_trancode, codigo_identificacao_carga, dados_todos_dominios: OperacaoProcessor(lista_trancode, codigo_identificacao_carga, dados_todos_dominios))
    ORIGINACAO = (2, lambda lista_trancode, codigo_identificacao_carga, dados_todos_dominios: OriginacaoProcessor(lista_trancode, codigo_identificacao_carga, dados_todos_dominios))
    PARCELA = (3, lambda lista_trancode, codigo_identificacao_carga, dados_todos_dominios: ParcelaProcessor(lista_trancode, codigo_identificacao_carga, dados_todos_dominios))
    MOVIMENTO_FINANCEIRO = (4, lambda lista_trancode, codigo_identificacao_carga, dados_todos_dominios: MovimentoFinanceiroProcessor(lista_trancode, codigo_identificacao_carga, dados_todos_dominios))


    def choose_method_process(self, lista_trancode, codigo_identificacao_carga, dados_todos_dominios):
        return self.value[1](lista_trancode, codigo_identificacao_carga, dados_todos_dominios)

    def get_instance(codigo_dominio):
        for val in Dominio:
            if val.value[0] == codigo_dominio:
                return val