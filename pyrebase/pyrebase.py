import requests
from requests.adapters import HTTPAdapter

from oauth2client.service_account import ServiceAccountCredentials

from .services import Auth, Database, Firestore, Storage


def initialize_app(config):
    return Firebase(config)


class Firebase:
    """ Firebase Interface """
    def __init__(self, config):
        self.api_key = config["apiKey"]
        self.auth_domain = config["authDomain"]
        self.database_url = config["databaseURL"]
        self.storage_bucket = config["storageBucket"]
        self.project_id = config["projectId"]
        self.credentials = None
        self.requests = requests.Session()
        if config.get("serviceAccount"):
            scopes = [
                'https://www.googleapis.com/auth/firebase.database',
                'https://www.googleapis.com/auth/userinfo.email',
                "https://www.googleapis.com/auth/cloud-platform",
                "https://firebasestorage.googleapis.com/",
            ]
            service_account_type = type(config["serviceAccount"])
            if service_account_type is str:
                self.credentials = ServiceAccountCredentials.from_json_keyfile_name(config["serviceAccount"], scopes)
            if service_account_type is dict:
                self.credentials = ServiceAccountCredentials.from_json_keyfile_dict(config["serviceAccount"], scopes)
        
        adapter = HTTPAdapter()

        for scheme in ('http://', 'https://'):
            self.requests.mount(scheme, adapter)

    def auth(self):
        return Auth(self.api_key, self.requests, self.credentials)

    def database(self):
        return Database(self.credentials, self.api_key, self.database_url, self.requests)
    
    def firestore(self, firebase_path: str="/", database_name="(default)", auth_id: str | None=None):
        return Firestore(self.requests, self.project_id, firebase_path, database_name, auth_id)

    def storage(self):
        return Storage(self.credentials, self.storage_bucket, self.requests)
