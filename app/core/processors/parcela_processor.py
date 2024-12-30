from app.core.domains.parcela import Parcela

class ParcelaProcessor:
    def __init__(self, lista_trancode, codigo_identificacao_carga, dados_todos_dominios):
        self.parcela = Parcela().trancode_to_object(lista_trancode, codigo_identificacao_carga, dados_todos_dominios).to_json()


    def processar(self):
        return self.parcela
