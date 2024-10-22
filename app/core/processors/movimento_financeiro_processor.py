from app.core.domains.movimento_financeiro import MovimentoFinanceiro
from datetime import datetime


class MovimentoFinanceiroProcessor:
    def __init__(self, lista_trancode, codigo_identificacao_carga):
        self.movimentos = list(map(lambda trancode: MovimentoFinanceiro(trancode, codigo_identificacao_carga), lista_trancode))

    def processar(self):
        self.movimentos.sort(key=self.__ordernar_movimentos, reverse=True)
        return list(map(lambda movimento: movimento.to_json(), self.movimentos))

    def __ordernar_movimentos(self, movimento_financeiro:MovimentoFinanceiro):
        return datetime.strptime(movimento_financeiro.data_evento,"%Y-%m-%d")






