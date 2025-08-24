import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources


@dlt.source
def rema_source():
    config: RESTAPIConfig = {
        "client": {
            "base_url": "hhttps://api.digital.rema1000.dk/api/v1/",
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
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_github",
        destination=dlt.destinations.duckdb(destination_name="rema"),
        dataset_name="rest_api_data",
    )

    load_info = pipeline.run(rema_source())
    print(load_info)
