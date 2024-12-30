

class OriginacaoOrganizer:
    def __init__(self, dados_dominio):
        self.originacao = dados_dominio

    def organizar(self):
        return self.originacao[0]
