import logging
import os
from collections import deque
from typing import Any, Dict, Iterable, Set

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
    """Collect all product group records by traversing category pages.

    Starting from the root daily-groceries page we follow any discovered
    category links ("Url" and "SeeMoreLink.Url") breadth‑first. Iteration
    continues until no further pages produce records containing
    "ProductGroupId" (i.e. the queue of discovered, unvisited paths is empty).

    For each ProductGroupId we keep the maximum observed TotalProducts and
    the first non-empty Heading.
    """

    def _extract_sub_paths(content: Iterable[dict]) -> Set[str]:
        paths: Set[str] = set()
        for rec in content:
            url = rec.get("Url")
            if isinstance(url, str) and url.startswith("/dagligvarer/"):
                paths.add(url)
            sml = rec.get("SeeMoreLink")
            if isinstance(sml, dict):
                sml_url = sml.get("Url")
                if isinstance(sml_url, str) and sml_url.startswith("/dagligvarer/"):
                    paths.add(sml_url)
        return paths

    def _merge_records(content: Iterable[dict]) -> int:
        """Merge product group records into "merged"; return count of *new* PG IDs."""
        new_count = 0
        for rec in content:
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
                new_count += 1
            else:
                if total > existing["TotalProducts"]:
                    existing["TotalProducts"] = total
                if not existing.get("Heading") and heading:
                    existing["Heading"] = heading
        return new_count

    _LOGGER.info("Collecting product group IDs (iterative traversal)")
    root_content, settings = _fetch_content(DAGLIG_URL)
    _LOGGER.info(
        "Root page fetched product_groups=%d has_settings=%s",
        sum(1 for r in root_content if isinstance(r.get("ProductGroupId"), str)),
        bool(settings),
    )

    merged: dict[str, dict] = {}
    total_new = _merge_records(root_content)
    _LOGGER.info(
        "Merged initial product groups new=%d total=%d", total_new, len(merged)
    )

    discovered = _extract_sub_paths(root_content)
    queue = deque(discovered)
    visited: Set[str] = set()
    _LOGGER.info("Initial discovered category paths=%d", len(discovered))

    pages_fetched = 0
    while queue:
        path = queue.popleft()
        if path in visited:
            continue
        visited.add(path)
        full_url = _normalize_url(path)
        _LOGGER.info(
            "Fetching category path=%s (visited=%d queue=%d)",
            path,
            len(visited),
            len(queue),
        )
        content, _ = _fetch_content(full_url)
        pages_fetched += 1
        if not content:
            _LOGGER.debug("No content returned path=%s", path)
            continue
        new_pg = _merge_records(content)
        _LOGGER.debug(
            "Processed path=%s records=%d new_product_groups=%d total_product_groups=%d",
            path,
            len(content),
            new_pg,
            len(merged),
        )
        # Discover further sub paths from this page
        new_paths = _extract_sub_paths(content)
        # Only enqueue unvisited & not already queued
        for p in new_paths:
            if p not in visited and p not in queue:
                queue.append(p)

    _LOGGER.info(
        "Traversal complete pages_fetched=%d distinct_paths=%d product_groups=%d",
        pages_fetched,
        len(visited),
        len(merged),
    )

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
        write_disposition={"disposition": "merge", "strategy": "scd2"},
        primary_key="id",
    )


if __name__ == "__main__":
    load_nemlig()
