import json

class MovimentoFinanceiro:

    def __init__(self,
                 custodia=None,
                 id_operacao=None,
                 id_evento=None,
                 data_evento=None,
                 codigo_identificacao_carga=None):
        self.custodia = custodia
        self.id_operacao = id_operacao
        self.id_evento = id_evento
        self.data_evento = data_evento
        self.codigo_identificacao_carga = codigo_identificacao_carga

    def trancode_to_object(self, trancode, codigo_identificacao_carga, dados_todos_dominios):
        self.custodia = trancode[0:2]
        self.id_operacao = trancode[2:11]
        self.id_evento = trancode[11:14]
        self.data_evento = trancode[14:24]
        self.codigo_identificacao_carga = codigo_identificacao_carga
        return self


    def to_json(self):
        return json.dumps(self.__dict__)