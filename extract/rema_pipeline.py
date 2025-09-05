import dlt
import os
from typing import Generator, List, Dict, Any
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
from dlt.sources.helpers import requests

def get_all_category_ids(departments_data):
    """
    Extract all category IDs from the departments JSON response.

    Args:
        departments_data: List of department dictionaries from the API response

    Returns:
        List of dictionaries with 'id' field containing category IDs
    """
    category_ids = []
    for department in departments_data:
        if 'categories' in department:
            for category in department['categories']:
                if 'id' in category:
                    category_ids.append({"id": category['id']})
    return category_ids

@dlt.resource()
def categories() -> Generator[List[Dict[str, Any]], Any, Any]:
    rema_departments_url = "https://api.digital.rema1000.dk/api/v1/catalog/store/1/departments-v2"
    response = requests.get(rema_departments_url)
    departments_data = response.json()

    yield get_all_category_ids(departments_data)

@dlt.source
def rema_source():
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.digital.rema1000.dk/api/v1/",
        },
        "resource_defaults": {
            "write_disposition": "replace",
        },
        "resources": [
            {
                "name": "departments",
                "endpoint": {
                    "path": "catalog/store/1/departments-v2",
                    "method": "GET",
                },
            },
            categories(),
            {
                "name": "products",
                "endpoint": {
                    "path": "https://flwdn2189e-dsn.algolia.net/1/indexes/aws-prod-products/query",
                    "method": "POST",
                    "headers": {
                        "x-algolia-agent": "Algolia for vanilla JavaScript 3.21.1",
                        "x-algolia-application-id": "FLWDN2189E",
                        "x-algolia-api-key": "fa20981a63df668e871a87a8fbd0caed"
                    },
                    "json": {
                        "query": "",
                        "hitsPerPage": 1000,
                        "facets": ["labels"],
                        "facetFilters": ["category_id:{resources.categories.id}"]
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)

def load_rema() -> None:
    data_folder_name = "data"
    os.makedirs(data_folder_name, exist_ok=True)

    pipeline = dlt.pipeline(
        pipeline_name="rest_api_rema",
        destination=dlt.destinations.duckdb(destination_name=f"{data_folder_name}/rema"),
        dataset_name="rest_api_data_rema",
    )

    load_info = pipeline.run(rema_source())
    print(load_info)

if __name__ == "__main__":
    load_rema()