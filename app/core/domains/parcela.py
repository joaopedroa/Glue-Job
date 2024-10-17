import json

class Parcela:

    def __init__(self, trancode, codigo_identificacao_carga):
        self.custodia = trancode[0:2]
        self.id_operacao = trancode[2:11]
        self.numero_plano = trancode[11:13]
        self.numero_parcela = trancode[11:13]
        self.data_vencimento_parcela = trancode[13:23]
        self.codigo_identificacao_carga = codigo_identificacao_carga

    def to_json(self):
        return json.dumps(self.__dict__)