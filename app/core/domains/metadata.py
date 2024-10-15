import json

class Metadata:

    def __init__(self, trancode, codigo_identificacao_carga):
        self.codigo_identificacao_carga = codigo_identificacao_carga
        self.quantidade_parcelas = trancode[0:3]

    def to_json(self):
        return json.dumps(self.__dict__)