import boto3
from app.core.enums.origem_carga import OrigemCarga

class WorkflowAdapter:
    def __init__(self, args_glue):
        self.glue_client = boto3.client("glue")
        self.args_glue = args_glue

    def recuperar_origem_carga_workflow(self):
        workflow_name = self.args_glue['WORKFLOW_NAME']
        workflow_run_id = self.args_glue['WORKFLOW_RUN_ID']
        workflow_params = self.glue_client.get_workflow_run_properties(Name=workflow_name,
                                        RunId=workflow_run_id)["RunProperties"]
        return OrigemCarga[workflow_params['ORIGEM_CARGA']]