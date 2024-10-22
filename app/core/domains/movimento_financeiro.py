import json

class MovimentoFinanceiro:

    def __init__(self, trancode, codigo_identificacao_carga):
        self.custodia = trancode[0:2]
        self.id_operacao = trancode[2:11]
        self.id_evento = trancode[11:14]
        self.data_evento = trancode[14:24]
        self.codigo_identificacao_carga = codigo_identificacao_carga

    def to_json(self):
        return json.dumps(self.__dict__)