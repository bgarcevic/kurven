import math
import os
from typing import Any, Dict, Generator, List

import dlt
from dlt.sources.helpers import requests
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

BASE_URL = "https://www.nemlig.com"
DAGLIG_URL = f"{BASE_URL}/dagligvarer?sortorder=navn&GetAsJson=1"


def get_product_group_ids():
    """
    Extract product group IDs and related settings from dagligvarer JSON data,
    similar to the provided JavaScript example.

    Args:
        daglig_data (dict): The JSON response from dagligvarer API

    Returns:
        dict: Contains productGroupIDs list, magicStamp, timeslot, and other constants
    """
    response = requests.get(DAGLIG_URL)
    daglig_data = response.json()
    content = daglig_data.get("content", [])
    page_size = 100
    product_group_ids = [
        {
            "ProductGroupId": e["ProductGroupId"],
            "TotalProducts": e.get("TotalProducts", 0),
            "TotalPages": math.ceil(e.get("TotalProducts", 1) / page_size),
        }
        for e in content
        if isinstance(e.get("ProductGroupId"), str)
    ]
    settings = daglig_data.get("Settings", {})
    magicStamp = settings.get("CombinedProductsAndSitecoreTimestamp", "")
    timeslot = settings.get("TimeslotUtc", "")
    magic1 = "1"
    magic2 = "0"
    order = "navn"

    return {
        "productGroupIDs": product_group_ids,
        "magicStamp": magicStamp,
        "timeslot": timeslot,
        "magic1": magic1,
        "magic2": magic2,
        "pageSize": page_size,
        "order": order,
    }


@dlt.resource()
def categories(product_group_ids) -> Generator[List[Dict[str, Any]], Any, Any]:
    product_groups = product_group_ids.get("productGroupIDs", [])
    yield product_groups


@dlt.source
def nemlig_source():
    product_group_ids = get_product_group_ids()

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://www.nemlig.com/",
        },
        "resource_defaults": {
            "write_disposition": "replace",
        },
        "resources": [
            {
                "name": "products",
                "endpoint": {
                    "method": "GET",
                    "path": f"webapi/{product_group_ids['magicStamp']}/{product_group_ids['timeslot']}/{product_group_ids['magic1']}/{product_group_ids['magic2']}/Products/GetByProductGroupId",
                    "paginator": {
                        "type": "page_number",
                        "page_param": "pageIndex",
                        "total_path": None,
                    },
                    "params": {
                        "sortorder": product_group_ids["order"],
                        "pageSize": product_group_ids["pageSize"],
                        "productGroupId": "{resources.categories.ProductGroupId}",
                    },
                    "headers": {"Referer": f"{BASE_URL}/dagligvarer?sortorder=navn"},
                },
            },
            categories(product_group_ids),
        ],
    }

    yield from rest_api_resources(config)


def load_nemlig() -> None:
    data_folder_name = "data"
    os.makedirs(data_folder_name, exist_ok=True)

    pipeline = dlt.pipeline(
        pipeline_name="rest_api_nemlig",
        destination=dlt.destinations.duckdb(
            destination_name=f"{data_folder_name}/nemlig"
        ),
        dataset_name="rest_api_data_nemlig",
    )

    load_info = pipeline.run(nemlig_source())
    print(load_info)


if __name__ == "__main__":
    load_nemlig()
