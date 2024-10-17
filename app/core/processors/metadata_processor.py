from app.core.domains.metadata import Metadata

class MetadataProcessor:
    def __init__(self, lista_trancode, codigo_identificacao_carga):
        self.metadatas = list(map(lambda trancode: Metadata(trancode, codigo_identificacao_carga).to_json(), lista_trancode))

    def processar(self):
        return self.metadatas[0]
