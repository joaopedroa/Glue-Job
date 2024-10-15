import json

class Operacao:

    def __init__(self, trancode, codigo_identificacao_carga):
        self.custodia = trancode[0:2]
        self.id_operacao = trancode[2:11]
        self.data_contratacao = trancode[11:21]

    def to_json(self):
        return json.dumps(self.__dict__)