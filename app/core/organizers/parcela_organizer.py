import json

from app.core.domains.parcela import Parcela


class ParcelaOrganizer:
    def __init__(self, dados_dominio):
        self.parcelas = list(map(lambda data: Parcela(**json.loads(data)), dados_dominio))

    def organizar(self):
        self.parcelas.sort(key=self.__ordernar_parcelas)
        parcelas_transformadas = list(map(lambda parcela: parcela.to_json(), self.parcelas))
        print(parcelas_transformadas)
        return parcelas_transformadas

    def __ordernar_parcelas(self, parcela: Parcela):
        return parcela.numero_plano, parcela.numero_parcela






