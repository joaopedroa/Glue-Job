
class Logger:
    def __init__(self, job_name, workflow_name, workflow_run_id):
        self.job_name = job_name
        self.workflow_name = workflow_name
        self.workflow_run_id = workflow_run_id

    def log(self, message):
        print(f"{message} - Info Runner: {self.job_name}, Workflow: {self.workflow_name}-{self.workflow_run_id}")