import json

class Originacao:

    def __init__(self,
                 custodia=None,
                 id_operacao=None,
                 meio_recebimento_valor=None,
                 codigo_identificacao_carga=None):
        self.custodia = custodia
        self.id_operacao = id_operacao
        self.meio_recebimento_valor = meio_recebimento_valor
        self.codigo_identificacao_carga = codigo_identificacao_carga

    def trancode_to_object(self, trancode, codigo_identificacao_carga, dados_todos_dominios):
        self.custodia = trancode[0:2]
        self.id_operacao = trancode[2:11]
        self.meio_recebimento_valor = trancode[11:23]
        self.codigo_identificacao_carga = codigo_identificacao_carga
        return self

    def to_json(self):
        return json.dumps(self.__dict__)