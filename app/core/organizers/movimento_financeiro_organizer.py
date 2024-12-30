from app.core.domains.movimento_financeiro import MovimentoFinanceiro
from datetime import datetime
import json

class MovimentoFinanceiroOrganizer:
    def __init__(self, dados_dominio):
        self.movimentos = list(map(lambda data: MovimentoFinanceiro(**json.loads(data)), dados_dominio))

    def organizar(self):
        self.movimentos.sort(key=self.__ordernar_movimentos, reverse=True)
        return list(map(lambda movimento: movimento.to_json(), self.movimentos))

    def __ordernar_movimentos(self, movimento_financeiro:MovimentoFinanceiro):
        return datetime.strptime(movimento_financeiro.data_evento,"%Y-%m-%d")






