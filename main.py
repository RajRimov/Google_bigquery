import os

from flask import Flask, request
from google.cloud import bigquery
from packages.pyGCP.pyGCP.functions import RequestEntity
from packages.pyGCP.pyGCP.bigquery import BigQueryRequest
from packages.pyGCP.pyGCP.gcs import GCSObject

app = Flask(__name__)


@app.route("/add_metadata", methods=["POST"])
def add_metadata():
    """HTTP cloud run
    Load a source table from a dataset, adds a new metadata column and
    save the result in a temporary table

    Args:
        request (str): A string of a JSON representing data associated with the call
            request['project_id'] (str): name of the GCP project
            request['dataset'] (str): destination dataset in BigQuery
            request['source_table'] (str): path to the source table
            request['destination_dataset'] (str): optional, destination dataset

    Returns:
        (string, int): A tuple of a json string containing the request status and a int for http status
            string[delta_dataset],string[delta_table]: if job succeed, name of the temp metadata enriched table
            string[message]: if job failed, human readable status message

        ex:
            job succeed: ('{"delta_dataset": "%tmp_dataset%", "table": "%tmp_table%"}', 200)
            job failed: ('{"message": "Job failed to add metadata"}', 500)

    """
    expected_keys = {"project_id", "dataset", "source_table"}

    req = RequestEntity(request.form.to_dict(), step="add_metadata")
    if not req.check(expected_keys):
        return req.abort(cause="missing_keys")

    # bq = BigQueryRequest(req["table_uri"])
    bq = BigQueryRequest(
        f"{req['project_id']}.{req['dataset']}.{req['source_table']}",
        step="add_metadata",
    )
    bq.destination_dataset = req.get("destination_dataset", bq.dataset)

    metadata = eval(req.get("metadata", {}))
    meta = ""
    for key in metadata:
        if "type" in metadata[key]:
            meta += f"\n, CAST('{metadata[key]['value']}' AS {metadata[key]['type']}) as {key} "
        else:
            meta += f"\n, '{metadata[key]['value']}' as {key} "

    query = f"""
        SELECT * {meta} FROM `{bq}`
    """

    try:
        bq.query(query)
    except Exception as e:
        return req.abort("Failed to add metadata to table", error=str(e))

    return req.complete(
        message="Successfully added metadata",
        params={
            "project_id": bq.destination_project,
            "dataset": bq.destination_dataset,
            "metadata_table": bq.destination_table,
        },
    )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
