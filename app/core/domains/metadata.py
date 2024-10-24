import json

class Metadata:

    def __init__(self, trancode, codigo_identificacao_carga):
        self.codigo_identificacao_carga = codigo_identificacao_carga
        self.quantidade_parcelas = int(trancode[0:3])
        self.quantidade_movimentos_financeiros = int(trancode[3:6])

    def to_json(self):
        return json.dumps(self.__dict__)