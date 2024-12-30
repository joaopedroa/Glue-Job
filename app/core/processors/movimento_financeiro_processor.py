from app.core.domains.movimento_financeiro import MovimentoFinanceiro
from datetime import datetime


class MovimentoFinanceiroProcessor:
    def __init__(self, lista_trancode, codigo_identificacao_carga, dados_todos_dominios):
        self.movimento = MovimentoFinanceiro().trancode_to_object(lista_trancode, codigo_identificacao_carga, dados_todos_dominios).to_json()

    def processar(self):
        return self.movimento





