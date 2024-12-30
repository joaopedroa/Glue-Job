from enum import Enum

from app.core.organizers.metadata_organizer import MetadataOrganizer
from app.core.organizers.movimento_financeiro_organizer import MovimentoFinanceiroOrganizer
from app.core.organizers.operacao_organizer import OperacaoOrganizer
from app.core.organizers.originacao_organizer import OriginacaoOrganizer
from app.core.organizers.parcela_organizer import ParcelaOrganizer
from app.core.processors.metadata_processor import MetadataProcessor
from app.core.processors.movimento_financeiro_processor import MovimentoFinanceiroProcessor
from app.core.processors.operacao_processor import OperacaoProcessor
from app.core.processors.originacao_processor import OriginacaoProcessor
from app.core.processors.parcela_processor import ParcelaProcessor


class Dominio(Enum):
    METADATA = (99, lambda lista_trancode, codigo_identificacao_carga, dados_todos_dominios: MetadataProcessor(lista_trancode, codigo_identificacao_carga, dados_todos_dominios), lambda dados_dominio: MetadataOrganizer(dados_dominio))
    OPERACAO = (1, lambda lista_trancode, codigo_identificacao_carga, dados_todos_dominios: OperacaoProcessor(lista_trancode, codigo_identificacao_carga, dados_todos_dominios), lambda dados_dominio: OperacaoOrganizer(dados_dominio))
    ORIGINACAO = (2, lambda lista_trancode, codigo_identificacao_carga, dados_todos_dominios: OriginacaoProcessor(lista_trancode, codigo_identificacao_carga, dados_todos_dominios), lambda dados_dominio: OriginacaoOrganizer(dados_dominio))
    PARCELA = (3, lambda lista_trancode, codigo_identificacao_carga, dados_todos_dominios: ParcelaProcessor(lista_trancode, codigo_identificacao_carga, dados_todos_dominios), lambda dados_dominio: ParcelaOrganizer(dados_dominio))
    MOVIMENTO_FINANCEIRO = (4, lambda lista_trancode, codigo_identificacao_carga, dados_todos_dominios: MovimentoFinanceiroProcessor(lista_trancode, codigo_identificacao_carga, dados_todos_dominios), lambda dados_dominio: MovimentoFinanceiroOrganizer(dados_dominio))


    def choose_method_process(self, lista_trancode, codigo_identificacao_carga, dados_todos_dominios):
        return self.value[1](lista_trancode, codigo_identificacao_carga, dados_todos_dominios)

    def choose_method_organizer(self, dados_dominio):
        return self.value[2](dados_dominio)

    def choose_method_pprocess(self, lista_trancode, codigo_identificacao_carga, dados_todos_dominios):
        return self.value[1](lista_trancode, codigo_identificacao_carga, dados_todos_dominios)

    def get_instance(codigo_dominio):
        for val in Dominio:
            if val.value[0] == codigo_dominio:
                return val