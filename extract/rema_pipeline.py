import dlt
import os
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources


@dlt.source
def rema_source():
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.digital.rema1000.dk/api/v1/",
        },
        "resources": [
            {
                "name": "departments",
                "endpoint": {
                    "path": "catalog/store/1/departments-v2",
                },
            }
        ],
    }

    yield from rest_api_resources(config)


def load_rema() -> None:
    data_folder_name = "data"
    os.makedirs(data_folder_name, exist_ok=True)

    pipeline = dlt.pipeline(
        pipeline_name="rest_api_github",
        destination=dlt.destinations.duckdb(destination_name=f"{data_folder_name}/rema"),
        dataset_name="rest_api_data_rema",
    )

    load_info = pipeline.run(rema_source())
    print(load_info)

if __name__ == "__main__":
    load_rema()