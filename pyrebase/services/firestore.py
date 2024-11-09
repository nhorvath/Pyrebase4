# import requests

from pyrebase.utils import raise_detailed_error

class Firestore:
    """Firebase Firestore"""
    def __init__(self, requests, project_id, firebase_path, database_name="(default)", auth_id=None):
        self.base_path = f"firestore.googleapis.com/v1/projects/{project_id}/databases/{database_name}/documents/{firebase_path}"
        self.headers = {}
        self.requests = requests
        if auth_id:
            self.headers["Authorization"] = f"Bearer {auth_id}"
        
    def authorize(self, auth_id: str):
        self.headers["Authorization"] = f"Bearer {auth_id}"
    
    def get_document(self, document: str):
        """Fetches the document from firestore database

        Args:
            document (str): document path relative to the base path passed on initialization
        """
        request_url = f"{self.base_path}/{document}"
        while "//" in request_url:
            request_url = request_url.replace('//', '/')
            
        request_url = "https://" + request_url
        response = self.requests.get(request_url, headers=self.headers)
        if response.status_code == 200:
            data = response.json()['fields']
            cleaned = self._process_document(data)
            return cleaned
        else:
            raise_detailed_error(response)
            
    def update_document(self, document, data):
        request_url = f"{self.base_path}/{document}"
        while "//" in request_url:
            request_url = request_url.replace('//', '/')
            
        request_url = "https://" + request_url
        self.requests.patch(request_url, headers=self.headers, json={"fields": data})
            
    def __process_value(self, dtype, value):
        processed = None
        match dtype:
            case 'stringValue':
                processed = str(value)
            case 'integerValue':
                processed = int(value)
            case 'booleanValue':
                processed = bool(value) 
            case 'mapValue':
                processed = self._process_document(value['fields'])
            case 'arrayValue':
                processed = [self.__process_value(d, v) for val in value["values"] for d, v in val.items()]
                
        return processed
    
    def _process_document(self, data: dict) -> dict:
        clean = {}
        for key in data:
            dtype = list(data[key].keys())[0]
            clean[key] = self.__process_value(dtype, data[key][dtype])
            
        return clean
