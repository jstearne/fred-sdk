import json
import requests
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

BASE_URL = "https://api.stlouisfed.org/fred/category"

# ---------------------------
# Schema Discovery Function
# ---------------------------
def schema(configuration: dict):
    return [
        {
            "table": "fred_categories",
            "primary_key": ["id"],
            "columns": {
                "id": "int",
                "name": "string",
                "parent_id": "int"
            }
        }
    ]

# ---------------------------
# Data Sync Function
# ---------------------------
def update(configuration: dict, state: dict):
    api_key = configuration.get("fred_api_key")

    if not api_key:
        log.warning("Missing or empty fred_api_key!")
        return

    params = {
        "api_key": api_key,
        "file_type": "json",
        "category_id": 125
    }

    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()

        category_data = response.json().get("categories", [])
        for category in category_data:
            yield op.upsert(
                table="fred_categories",
                data={
                    "id": int(category["id"]),
                    "name": category["name"],
                    "parent_id": int(category["parent_id"])
                }
            )

        yield op.checkpoint(state)

    except requests.RequestException as e:
        log.warning(f"API request failed: {str(e)}")

# ---------------------------
# Connector Entry Point
# ---------------------------
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json") as f:
        test_config = json.load(f)

    print(f"API Key from configuration.json: {test_config.get('fred_api_key')}")

    connector.debug(
        config=test_config,
        state={}
    )