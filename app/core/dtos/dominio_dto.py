import json

class DominioDTO:

    def __init__(self, dominio, lista_trancode):
        self.dominio = dominio
        self.lista_trancode = lista_trancode

    def to_json(self):
        return json.dumps(self.__dict__)