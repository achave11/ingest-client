import os

import ingest.api.ingestapi as ingestapi
import json


class JsonToIngest:
    DEFAULT_INGEST_URL = os.environ.get('INGEST_API', 'http://api.ingest.dev.data.humancellatlas.org')

    entity_type_endpoints = {"project": "projects",
                             "biomaterial": "biomaterials",
                             "process": "processes",
                             "protocol": "protocols",
                             "file": "files"}
    id_to_url_mappings = {}

    def __init__(self, options=None):
        self.ingestUrl = options.ingest if options and options.ingest else os.path.expandvars(self.DEFAULT_INGEST_URL)
        self.ingest_api = ingestapi.IngestApi(self.ingestUrl)

    def submit(self, token, json_file):
        self._create_entities(self._create_submission(token), self._load_json_data(json_file), token)
        print(str(self.id_to_url_mappings))

    def _create_submission(self, token):
        return self.ingest_api.createSubmission(self._get_bearer_token(token))

    @staticmethod
    def _load_json_data(json_file):
        with open(json_file) as json_data:
            d = json.load(json_data)
        return d

    @staticmethod
    def _get_bearer_token(token):
        return "Bearer " + token

    def _create_entities(self, submission_url, data, token):
        for entry_set in data:
            if 'schema_type' in entry_set:
                entity_type = entry_set['schema_type']
                entity_type_endpoint = self.entity_type_endpoints[entity_type]
                if 'content' in entry_set:
                    content = entry_set['content']
                    for entry_json in content:
                        json_string = json.dumps(entry_json, indent=4)
                        response_json = self.ingest_api.createEntity(submission_url, json_string, entity_type_endpoint,
                                                                     self._get_bearer_token(token))
                        self._store_mapping(entity_type, entry_json, response_json)

    def _store_mapping(self, entity_type, entry_json, response_json):
        if entity_type + "_core" in entry_json:
            entity_json_core = entry_json[entity_type + "_core"]
            if entity_type + "_id" in entity_json_core:
                spreadsheet_id = entity_type + ":" + entity_json_core[entity_type + "_id"]
                ingest_url = response_json['_links']['self']['href']
                self.id_to_url_mappings.update({spreadsheet_id: ingest_url})
