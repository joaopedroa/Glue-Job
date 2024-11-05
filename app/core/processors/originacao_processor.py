from app.core.domains.originacao import Originacao

class OriginacaoProcessor:
    def __init__(self, lista_trancode, codigo_identificacao_carga, dados_todos_dominios):
        self.originacoes = list( map(lambda trancode: Originacao(trancode, codigo_identificacao_carga, dados_todos_dominios).to_json(), lista_trancode))

    def processar(self):
        return self.originacoes[0]
