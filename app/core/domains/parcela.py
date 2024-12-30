import json

class Parcela:

    def __init__(self,
                 custodia=None,
                 id_operacao=None,
                 numero_plano=None,
                 numero_parcela=None,
                 data_vencimento_parcela=None,
                 codigo_identificacao_carga=None):
        self.custodia = custodia
        self.id_operacao = id_operacao
        self.numero_plano = numero_plano
        self.numero_parcela = numero_parcela
        self.data_vencimento_parcela = data_vencimento_parcela
        self.codigo_identificacao_carga = codigo_identificacao_carga

    def trancode_to_object(self, trancode, codigo_identificacao_carga, dados_todos_dominios):
        self.custodia = trancode[0:2]
        self.id_operacao = trancode[2:11]
        self.numero_plano = trancode[11:13]
        self.numero_parcela = trancode[11:13]
        self.data_vencimento_parcela = trancode[13:23]
        self.codigo_identificacao_carga = codigo_identificacao_carga
        return self


    def to_json(self):
        return json.dumps(self.__dict__)