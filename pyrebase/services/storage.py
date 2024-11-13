from uuid import uuid4
from google.cloud import storage
from urllib.parse import quote
from pyrebase.utils import raise_detailed_error
import requests


class Storage:
    """ Storage Service """
    def __init__(self, credentials, storage_bucket, requests):
        self.storage_bucket = "https://firebasestorage.googleapis.com/v0/b/" + storage_bucket
        self.credentials = credentials
        self.requests = requests
        self.path = ""
        if credentials:
            client = storage.Client(credentials=credentials, project=storage_bucket)
            self.bucket = client.get_bucket(storage_bucket)

    def child(self, *args):
        new_path = "/".join(args)
        if self.path:
            self.path += "/{}".format(new_path)
        else:
            if new_path.startswith("/"):
                new_path = new_path[1:]
            self.path = new_path
        return self

    def put(self, file, token=None, content_type=None):
        # reset path
        path = self.path
        self.path = None
        if isinstance(file, str):
            file_object = open(file, 'rb')
        else:
            file_object = file
        request_ref = self.storage_bucket + "/o?name={0}".format(path)
        if token:
            headers = {"Authorization": "Firebase " + token}
            request_object = self.requests.post(request_ref, headers=headers, data=file_object)
            raise_detailed_error(request_object)
            return request_object.json()
        elif self.credentials:
            blob = self.bucket.blob(path)

            # Add metadata to enable file previews in console
            blob.metadata = {"firebaseStorageDownloadTokens": str(uuid4())}
            if isinstance(file, str):
                return blob.upload_from_filename(filename=file)
            else:
                # If the file is not a string we need to patch the blob after upload to set the content type
                blob.upload_from_string(file)
                if content_type:
                    blob.content_type = content_type
                blob.patch()
        else:
            request_object = self.requests.post(request_ref, data=file_object)
            raise_detailed_error(request_object)
            return request_object.json()

    def delete(self, name, token):
        if self.credentials:
            self.bucket.delete_blob(name)
        else:
            request_ref = self.storage_bucket + "/o/?name={0}".format(name)
            if token:
                headers = {"Authorization": "Firebase " + token}
                request_object = self.requests.delete(request_ref, headers=headers)
            else:
                request_object = self.requests.delete(request_ref)
            raise_detailed_error(request_object)

    def download(self, path, filename, token=None):
        # remove leading backlash
        url = self.get_url(token)
        if path.startswith('/'):
            path = path.lstrip('/')
        if self.credentials:
            blob = self.bucket.get_blob(path)
            if not blob is None:
                blob.download_to_filename(filename)
        elif token:
             headers = {"Authorization": "Firebase " + token}
             r = requests.get(url, stream=True, headers=headers)
             if r.status_code == 200:
                 with open(filename, 'wb') as f:
                    for chunk in r:
                         f.write(chunk)
        else:
            r = requests.get(url, stream=True)
            if r.status_code == 200:
                with open(filename, 'wb') as f:
                    for chunk in r:
                        f.write(chunk)

    def get_url(self, token):
        path = self.path if self.path else ''
        self.path = None
        if path.startswith('/'):
            path = path.lstrip('/')
        if token:
            return "{0}/o/{1}?alt=media&token={2}".format(self.storage_bucket, quote(path, safe=''), token)
        return "{0}/o/{1}?alt=media".format(self.storage_bucket, quote(path, safe=''))

    def list_files(self):
        return self.bucket.list_blobs()
