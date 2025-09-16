import logging
import os
from typing import Any, Dict

import dlt
from dlt.sources.helpers import requests
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources

BASE_URL = "https://www.nemlig.com"
DAGLIG_URL = f"{BASE_URL}/dagligvarer?sortorder=navn&GetAsJson=1"

# Logging setup (module-level). Format similar to provided example.
_LOGGER = logging.getLogger("nemlig")
if not _LOGGER.handlers:
    _LOGGER.setLevel(logging.INFO)
    _handler = logging.StreamHandler()
    _formatter = logging.Formatter(
        fmt="%(asctime)s|[%(levelname)s]|%(process)d|%(thread)d|%(name)s|%(module)s.py|%(funcName)s:%(lineno)d|%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _handler.setFormatter(_formatter)
    _LOGGER.addHandler(_handler)
    _LOGGER.propagate = False


def _normalize_url(path: str) -> str:
    if not path:
        return ""
    url = path if path.startswith("http") else f"{BASE_URL}{path}"
    if "GetAsJson=1" not in url:
        joiner = "&" if "?" in url else "?"
        url = f"{url}{joiner}sortorder=navn&GetAsJson=1"
    _LOGGER.debug("Normalized URL path=%s -> %s", path, url)
    return url


def _fetch_content(url: str) -> tuple[list[dict], dict]:
    try:
        _LOGGER.info("Fetching content url=%s", url)
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        js = r.json()
        _LOGGER.debug(
            "Fetched JSON keys=%s status=%s",
            list(js.keys()),
            getattr(r, "status_code", None),
        )
        return js.get("content", []) or [], js.get("Settings", {}) or {}
    except Exception as ex:
        _LOGGER.warning("Failed to fetch url=%s error=%s", url, ex)
        return [], {}


def get_product_group_ids() -> Dict[str, Any]:
    _LOGGER.info("Collecting product group IDs from root page")
    root_content, settings = _fetch_content(DAGLIG_URL)
    _LOGGER.info(
        "Root page fetched product_groups=%d has_settings=%s",
        len(root_content),
        bool(settings),
    )

    # Collect sub paths (Url + SeeMoreLink.Url) in one pass
    sub_paths = {
        cand
        for rec in root_content
        for cand in (
            (rec.get("Url") if isinstance(rec.get("Url"), str) else None),
            (
                rec.get("SeeMoreLink", {}).get("Url")
                if isinstance(rec.get("SeeMoreLink"), dict)
                else None
            ),
        )
        if isinstance(cand, str) and cand.startswith("/dagligvarer/")
    }
    _LOGGER.info("Discovered sub category paths count=%d", len(sub_paths))

    # Accumulate records from root + all sub pages
    all_records: list[dict] = list(root_content)
    for path in sub_paths:
        full_url = _normalize_url(path)
        _LOGGER.info("Fetching sub category path=%s", path)
        content, _ = _fetch_content(full_url)
        if content:
            all_records.extend(content)
            _LOGGER.debug(
                "Appended records=%d from=%s total_records=%d",
                len(content),
                path,
                len(all_records),
            )

    # Merge product groups: keep max TotalProducts, keep first non-empty Heading
    merged: dict[str, dict] = {}
    for rec in all_records:
        pg_id = rec.get("ProductGroupId")
        if not isinstance(pg_id, str):
            continue
        total = rec.get("TotalProducts") or 0
        heading = rec.get("Heading") or ""
        existing = merged.get(pg_id)
        if not existing:
            merged[pg_id] = {
                "ProductGroupId": pg_id,
                "TotalProducts": total,
                "Heading": heading,
            }
        else:
            if total > existing["TotalProducts"]:
                existing["TotalProducts"] = total
            if not existing.get("Heading") and heading:
                existing["Heading"] = heading
    _LOGGER.info("Merged distinct product groups=%d", len(merged))

    return {
        "productGroupIDs": list(merged.values()),
        "magicStamp": settings.get("CombinedProductsAndSitecoreTimestamp", ""),
        "timeslot": settings.get("TimeslotUtc", ""),
        "magic1": "1",
        "magic2": "0",
        "pageSize": 100,
        "order": "navn",
    }


@dlt.resource()
def categories(product_group_ids: Dict[str, Any]):
    yield product_group_ids["productGroupIDs"]


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
                "write_disposition": {"disposition": "merge"},
                "primary_key": "id",
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

    products = pipeline.dataset().products
    pipeline.run(
        products.iter_arrow(chunk_size=1000),
        table_name="products_history",
        write_disposition="merge",
        primary_key="id",
    )


if __name__ == "__main__":
    # load_nemlig()
    print(get_product_group_ids())
