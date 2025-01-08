class Dados():

    def __init__(self, nome, idade):
        self.nome = nome
        self.idade = idade

    def to_json(self):
        return self.__dict__
