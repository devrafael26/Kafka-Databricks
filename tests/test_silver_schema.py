import json
from pathlib import Path
from jsonschema import validate


BASE_DIR = Path(__file__).parent


def test_silver_schema():

    silver_record = {
        "orderid": 75991,
        "orderdate": "2017-06-20T10:25:19",
        "itemid": "Item_140",
        "orderunits": 9,
        "city": "City_39",
        "state": "State_13",
        "zipcode": 33867,
        "bronze_processing_timestamp": "2026-07-10T12:00:00",
        "silver_processing_timestamp": "2026-07-10T12:01:00"
    }

    with open(
        BASE_DIR / "schemas" / "silver_orders_schema.json",
        "r"
    ) as file:
        schema = json.load(file)

    validate(
        instance=silver_record,
        schema=schema
    )
