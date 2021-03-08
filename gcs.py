
from google.cloud import storage


class GCSObject(str):

    def __init__(self, uri=""):
        self.base, self.bucket, self.path = self.parse_uri(uri)

    def parse_uri(self, uri):
        uri = uri.lstrip("gs://").replace("//", "/").split("/", 1)
        if len(uri) > 1:
            return ("gs://", uri[0], uri[1])
        else:
            return ("gs://", uri[0], "")

    def read(self) -> bytes:
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket)
        return bucket.blob(self.path).download_as_string()

    def write(self, content: bytes):
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.bucket)
        blob = bucket.blob(self.path)
        blob.upload_from_string(content)