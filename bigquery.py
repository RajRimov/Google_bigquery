import uuid
import re
import os
from google.cloud import bigquery


class BigQueryRequest:


    def __init__(self, uri="", step="", params={}):
        self.project, self.dataset, self.table = self.from_uri(uri)
        self.step = step
        self.params = self.set_params(params)
        self.overwrite = False
        (
            self.destination_project,
            self.destination_dataset,
            self.destination_table,
        ) = self.__set_destination()

    def __repr__(self):
        return str(f"{self.project}.{self.dataset}.{self.table}")

    def __set_destination(self):
        if self.overwrite:
            return (str(self.project), str(self.dataset), str(self.table))
        else:
            source_table_name = re.sub("__[a-z\_]*__[a-z0-9]{32}", "", self.table)
            destination_table = f"{source_table_name}__{self.step}__{uuid.uuid4().hex}"
            return (str(self.project), str(self.dataset), destination_table)

    @classmethod
    def from_uri(self, uri):
        uri = uri.split(".")
        if len(uri) > 2:
            return (uri[0], uri[1], uri[2])
        else:
            return (os.environ.get("GCP_PROJECT"), uri[0], uri[1])

    def set_params(self, params):
        default = {
            "create_disposition": "CREATE_IF_NEEDED",
        }
        default.update(params)
        return default

    def destination(self):
        return f"{self.destination_project}.{self.destination_dataset}.{self.destination_table}"

    def query(self, request):
        client = bigquery.Client()

        job_config = bigquery.QueryJobConfig(
            destination=self.destination(), **self.params
        )

        query_job = client.query(
            request,
            job_config=job_config,
            job_id_prefix=f"{self.step}_",
        )
        return query_job.result()


class BigQueryLoad(BigQueryRequest):
    def set_params(self, params):
        default = {}
        default.update(params)
        return default

    def load(self, file):
        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig(**self.params)
        dataset = client.dataset(self.destination_dataset)

        query_job = client.load_table_from_uri(
            file,
            dataset.table(self.destination_table),
            job_config=job_config,
            job_id_prefix=f"{self.step}_",
        )
        return query_job.result()
