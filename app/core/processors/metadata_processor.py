from app.core.domains.metadata import Metadata


class MetadataProcessor:
    def __init__(self, lista_trancode, codigo_identificacao_carga, dados_todos_dominios):
        self.metadata = Metadata().trancode_to_object(lista_trancode, codigo_identificacao_carga, dados_todos_dominios).to_json()

    def processar(self):
        return self.metadata


