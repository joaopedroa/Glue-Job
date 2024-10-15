import json

class Originacao:

    def __init__(self, trancode, codigo_identificacao_carga):
        self.custodia = trancode[0:2]
        self.id_operacao = trancode[2:11]
        self.meio_recebimento_valor = trancode[11:23]

    def to_json(self):
        return json.dumps(self.__dict__)