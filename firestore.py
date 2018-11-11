import typing

from firebase_admin import firestore
from firebase_admin import storage
from google.cloud.storage import Blob


class FireBaseManager:

    def __init__(self, app):
        self.bucket = storage.bucket(app=app)
        self.database = firestore.client(app)

    def upload_image(self, location: str, label: str, image_bytes, metadata: dict) -> typing.Optional[str]:
        blob = Blob(f"{location}/{label}", self.bucket)
        blob.metadata = metadata
        blob.upload_from_file(image_bytes, content_type="image/png")
        blob.make_public()
        return blob.public_url

    def upload_data(self, location: str, document_id: str, data: typing.Dict):
        collection: firestore.firestore.CollectionReference = self.database.collection(str(location))
        try:
            document: firestore.firestore.DocumentReference = collection.document(str(document_id))
            return document.set(data)
        except:
            return

    def get_blob_url(self, location: str, label: str) -> str:
        blob = self.bucket.get_blob(f"{location}/{label}")
        if blob:
            return blob.public_url
        return ""

    def set_blob_url(self, location: str, label: str) -> str:
        blob = Blob(f"{location}/{label}", self.bucket)
        return blob.public_url
