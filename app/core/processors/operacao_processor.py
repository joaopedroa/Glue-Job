from app.core.domains.operacao import Operacao


class OperacaoProcessor:
    def __init__(self, lista_trancode, codigo_identificacao_carga, dados_todos_dominios):
        self.operacoes = list(map(lambda trancode: Operacao(trancode, codigo_identificacao_carga, dados_todos_dominios).to_json(), lista_trancode))

    def processar(self):
        return self.operacoes[0]
