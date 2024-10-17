from app.core.domains.operacao import Operacao


class OperacaoProcessor:
    def __init__(self, lista_trancode, codigo_identificacao_carga):
        self.operacoes = list(map(lambda trancode: Operacao(trancode, codigo_identificacao_carga).to_json(), lista_trancode))

    def processar(self):
        return self.operacoes[0]
