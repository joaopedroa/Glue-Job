from app.core.domains.operacao import Operacao


class OperacaoProcessor:
    def __init__(self, lista_trancode, codigo_identificacao_carga, dados_todos_dominios):
        self.operacao = Operacao().trancode_to_object(lista_trancode, codigo_identificacao_carga, dados_todos_dominios).to_json()


    def processar(self):
        return self.operacao
