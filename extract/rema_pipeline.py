import os
from typing import Any, Dict, Generator, List
import logging

import dlt
from dlt.sources.helpers import requests
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

# Create a logger and set handler
logger = logging.getLogger("dlt")
logger.setLevel(logging.INFO)

@dlt.source
def rema_source():
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://cphapp.rema1000.dk/api/v1/",
        },
        "resources": [
            {
                "name": "catalog",
                "endpoint": {
                    "path": "catalog/store/1/withchildren",
                    "method": "GET",
                    "paginator": "single_page",
                },
            },
        ],
    }

    yield from rest_api_resources(config)

def load_rema() -> None:
    data_folder_name = "../data"
    os.makedirs(data_folder_name, exist_ok=True)

    pipeline = dlt.pipeline(
        pipeline_name="rest_api_rema",
        destination=dlt.destinations.duckdb(
            destination_name=f"{data_folder_name}/rema"
        ),
        dataset_name="rest_api_data_rema",
    )

    pipeline.run(rema_source())
    print("Pipeline run completed successfully.")
    print(f"Last Trace: {pipeline.last_trace}")


if __name__ == "__main__":
    load_rema()
