from app.core.domains.parcela import Parcela

class ParcelaProcessor:
    def __init__(self, lista_trancode, codigo_identificacao_carga, dados_todos_dominios):
        self.parcelas = list(map(lambda trancode: Parcela(trancode, codigo_identificacao_carga, dados_todos_dominios),lista_trancode))

    def processar(self):
        self.parcelas.sort(key=self.__ordernar_parcelas)
        return list(map(lambda parcela: parcela.to_json(), self.parcelas))

    def __ordernar_parcelas(self, parcela:Parcela):
        return parcela.numero_plano, parcela.numero_parcela






