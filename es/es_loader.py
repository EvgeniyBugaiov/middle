import json
import requests
from urllib.parse import urljoin
import logging

logger = logging.getLogger()


class ESLoader:
    def __init__(self, url):
        self.url = url

    def _get_es_bulk_query(self, rows, index_name):
        prepared_query = []
        for row in rows:
            prepared_query.extend([
                json.dumps(
                    {'index': {'_index': index_name, '_id': row['id']}}),
                json.dumps(row, ensure_ascii=False)
            ])
        return prepared_query

    def load_to_es(self, records, index_name):
        prepared_query = self._get_es_bulk_query(records, index_name)
        str_query = '\n'.join(prepared_query) + '\n'

        response = requests.post(
            urljoin(self.url, '_bulk'),
            data=str_query.encode('utf-8'),
            headers={'Content-Type': 'application/x-ndjson'}
        )
        json_response = json.loads(response.content.decode())
        for item in json_response['items']:
            error_message = item['index'].get('error')
            if error_message:
                logger.error(error_message)
