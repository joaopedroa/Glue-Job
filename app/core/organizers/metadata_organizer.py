

class MetadataOrganizer:
    def __init__(self, dados_dominio):
        self.metadata = dados_dominio

    def organizar(self):
        return self.metadata[0]
