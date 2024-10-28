
class S3Adapter:
    def __init__(self, glue_context):
        self.glue_context = glue_context

    def extrair(self, path, transformation_ctx):
        return (
            self.glue_context.create_dynamic_frame.from_options(
                connection_type="s3",
                format="csv",
                connection_options={"paths": [path], "recurse": False},
                transformation_ctx=transformation_ctx))

    def salvar(self, dynamic_frame, path):
        self.glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options={
                "path": path
            },
            format_options={
                "writeHeader": 'false',
                "quoteChar": '-1',
                "separator": '|'
            },
            format="csv"
        )