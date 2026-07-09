import json
from pathlib import Path
from jsonschema import validate


BASE_DIR = Path(__file__).parent


def test_order_event_contract():

    # carrega evento de exemplo
    with open(
        BASE_DIR / "fixtures" / "order_event.json",
        "r"
    ) as file:
        event = json.load(file)

    # carrega schema esperado
    with open(
        BASE_DIR / "schemas" / "order_event_schema.json",
        "r"
    ) as file:
        schema = json.load(file)

    # valida contrato
    validate(
        instance=event,
        schema=schema
    )
