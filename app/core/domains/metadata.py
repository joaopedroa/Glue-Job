import json

class Metadata:

    def __init__(self,
                 codigo_identificacao_carga=None,
                 quantidade_parcelas=None,
                 quantidade_movimentos_financeiros=None,
                 quantidade_parcelas_recebidas=None
                 ):
        self.codigo_identificacao_carga = codigo_identificacao_carga
        self.quantidade_parcelas = quantidade_parcelas
        self.quantidade_movimentos_financeiros = quantidade_movimentos_financeiros
        self.quantidade_parcelas_recebidas = quantidade_parcelas_recebidas

    def trancode_to_object(self, trancode, codigo_identificacao_carga, dados_todos_dominios):
        self.codigo_identificacao_carga = codigo_identificacao_carga
        self.quantidade_parcelas = int(trancode[0:3])
        self.quantidade_movimentos_financeiros = int(trancode[3:6])
        self.quantidade_parcelas_recebidas = self.__recuperar_dados_parcelas(dados_todos_dominios)
        return self

    def to_json(self):
        return json.dumps(self.__dict__)

    def __recuperar_dados_parcelas(self, dados_todos_dominios):
        lista_map = list(map(lambda x: json.loads(x), dados_todos_dominios))
        lista =  list(filter(lambda x: x['dominio'] == 'PARCELA', lista_map))
        if(len(lista) > 0):
            parcela_dto = lista[0]
            return len(parcela_dto['lista_trancode'])
        return 0
