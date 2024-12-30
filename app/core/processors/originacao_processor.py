from app.core.domains.originacao import Originacao

class OriginacaoProcessor:
    def __init__(self, lista_trancode, codigo_identificacao_carga, dados_todos_dominios):
        self.originacao = Originacao().trancode_to_object(lista_trancode, codigo_identificacao_carga, dados_todos_dominios).to_json()

    def processar(self):
        return self.originacao

