from app.core.domains.parcela import Parcela
from app.core.domains.parcela_controle_iof import ParcelaControleIof


class ParcelaControleIofProcessor:
    def __init__(self, lista_trancode, codigo_identificacao_carga, dados_todos_dominios):
        self.parcela = ParcelaControleIof().trancode_to_object(lista_trancode, codigo_identificacao_carga, dados_todos_dominios).to_json()


    def processar(self):
        return self.parcela
