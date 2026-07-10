def test_orderunits_not_negative():

    orders = [
        {
            "orderid": 1,
            "orderunits": 10
        },
        {
            "orderid": 2,
            "orderunits": 5
        }
    ]

    invalid_orders = [
        order for order in orders
        if order["orderunits"] < 0
    ]

    assert len(invalid_orders) == 0


def test_city_not_null():

    orders = [
        {
            "orderid": 1,
            "city": "City_39"
        },
        {
            "orderid": 2,
            "city": "City_10"
        }
    ]

    invalid_orders = [
        order for order in orders
        if order["city"] is None
    ]

    assert len(invalid_orders) == 0


def test_orderid_unique():

    orders = [
        {"orderid": 1},
        {"orderid": 2},
        {"orderid": 3}
    ]

    order_ids = [
        order["orderid"]
        for order in orders
    ]

    assert len(order_ids) == len(set(order_ids))
