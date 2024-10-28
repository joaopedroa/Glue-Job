
class DynamoAdapter:
    def __init__(self, glue_context):
        self.glue_context = glue_context

    def salvar(self, dynamic_frame):
        self.glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="dynamodb",
            connection_options={
                "dynamodb.output.tableName": "operacoes",
                "dynamodb.throughput.write.percent": "1.0"
            }
        )
