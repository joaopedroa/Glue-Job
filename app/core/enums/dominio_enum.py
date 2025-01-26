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
from app.core.processors.parcela_controle_iof_processor import ParcelaControleIofProcessor
from app.core.processors.parcela_processor import ParcelaProcessor
from app.core.strategies.democratizacao.metadata_democratizer import MetadataDemocratizer
from app.core.strategies.democratizacao.movimento_financeiro_democratizer import MovimentoFinanceiroDemocratizer
from app.core.strategies.democratizacao.operacao_democratizer import OperacaoDemocratizer
from app.core.strategies.democratizacao.originacao_democratizer import OriginacaoDemocratizer
from app.core.strategies.democratizacao.parcela_democratizer import ParcelaDemocratizer


class Dominio(Enum):
    METADATA = (99, lambda lista_trancode, codigo_identificacao_carga, dados_todos_dominios: MetadataProcessor(
        lista_trancode, codigo_identificacao_carga, dados_todos_dominios),
                lambda dados_dominio: MetadataOrganizer(dados_dominio),
                lambda data_frame: MetadataDemocratizer(data_frame))
    OPERACAO = (1, lambda lista_trancode, codigo_identificacao_carga, dados_todos_dominios: OperacaoProcessor(
        lista_trancode, codigo_identificacao_carga, dados_todos_dominios),
                lambda dados_dominio: OperacaoOrganizer(dados_dominio),
                lambda data_frame: OperacaoDemocratizer(data_frame))
    ORIGINACAO = (2, lambda lista_trancode, codigo_identificacao_carga, dados_todos_dominios: OriginacaoProcessor(
        lista_trancode, codigo_identificacao_carga, dados_todos_dominios),
                  lambda dados_dominio: OriginacaoOrganizer(dados_dominio),
                  lambda data_frame: OriginacaoDemocratizer(data_frame))
    PARCELA = (3,
               lambda lista_trancode, codigo_identificacao_carga, dados_todos_dominios: ParcelaProcessor(lista_trancode,
                                                                                                         codigo_identificacao_carga,
                                                                                                         dados_todos_dominios),
               lambda dados_dominio: ParcelaOrganizer(dados_dominio),
               lambda data_frame: ParcelaDemocratizer(data_frame))
    PARCELA_CONTROLE_IOF = (100,
                            lambda lista_trancode, codigo_identificacao_carga,
                                   dados_todos_dominios: ParcelaControleIofProcessor(
                                lista_trancode, codigo_identificacao_carga, dados_todos_dominios),
                            lambda dados_dominio: ParcelaOrganizer(dados_dominio),
                            lambda data_frame: ParcelaDemocratizer(data_frame))
    MOVIMENTO_FINANCEIRO = (4, lambda lista_trancode, codigo_identificacao_carga,
                                      dados_todos_dominios: MovimentoFinanceiroProcessor(lista_trancode,
                                                                                         codigo_identificacao_carga,
                                                                                         dados_todos_dominios),
                            lambda dados_dominio: MovimentoFinanceiroOrganizer(dados_dominio),
                            lambda data_frame: MovimentoFinanceiroDemocratizer(data_frame))

    def choose_method_process(self, lista_trancode, codigo_identificacao_carga, dados_todos_dominios):
        return self.value[1](lista_trancode, codigo_identificacao_carga, dados_todos_dominios)

    def choose_method_organizer(self, dados_dominio):
        return self.value[2](dados_dominio)

    def choose_method_democratizer(self, data_frame):
        return self.value[3](data_frame)

    def get_instance(codigo_dominio):
        for val in Dominio:
            if val.value[0] == codigo_dominio:
                return val
