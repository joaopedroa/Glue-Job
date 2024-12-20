import json

class ArquivoProcessado:

    def __init__(self, bucket, path, path_file):
        self.bucket = bucket
        self.path = path
        self.path_file = path_file
