import boto3
from app.core.enums.origem_carga import OrigemCarga

class AwsAdapter:
    def __init__(self, args_glue):
        self.glue_client = boto3.client("glue")
        self.args_glue = args_glue

    def recuperar_origem_carga_workflow(self):
        workflow_name = self.args_glue['WORKFLOW_NAME']
        workflow_run_id = self.args_glue['WORKFLOW_RUN_ID']
        workflow_params = self.glue_client.get_workflow_run_properties(Name=workflow_name,
                                        RunId=workflow_run_id)["RunProperties"]
        return OrigemCarga[workflow_params['ORIGEM_CARGA']]

    def recuperar_path_arquivos_processados(self):
        job_name = self.args_glue['JOB_NAME']
        response = self.glue_client.get_job(JobName=job_name)
        temp_dir = response['Job']['DefaultArguments']['--TempDir']
        job_run_id = self.args_glue['JOB_RUN_ID']
        return f"{temp_dir}partitionlisting/{job_name}/{job_run_id}/"