import csv

from config.schema import CANONICAL_FIELDS
from data_ingestion.producer import iter_source_events


def _write_csv(path, rows):
    fields = ["event_time", "event_type", "product_id", "category_id",
              "category_code", "brand", "price", "user_id", "user_session"]
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def test_producer_yields_canonical_events_and_skips_invalid(tmp_path):
    csv_path = tmp_path / "events.csv"
    _write_csv(csv_path, [
        {"event_time": "2019-10-01 10:00:00 UTC", "event_type": "purchase", "product_id": "p1",
         "category_id": "1", "category_code": "books", "brand": "b", "price": "12.5",
         "user_id": "u1", "user_session": "s1"},
        # invalid: unsupported event_type -> dropped at the edge
        {"event_time": "2019-10-01 10:01:00 UTC", "event_type": "search", "product_id": "p2",
         "category_id": "1", "category_code": "books", "brand": "b", "price": "1",
         "user_id": "u1", "user_session": "s1"},
        # invalid: missing product_id -> dropped
        {"event_time": "2019-10-01 10:02:00 UTC", "event_type": "view", "product_id": "",
         "category_id": "1", "category_code": "books", "brand": "b", "price": "1",
         "user_id": "u1", "user_session": "s1"},
    ])

    events = list(iter_source_events(csv_path, loop=False))
    assert len(events) == 1
    assert set(events[0].keys()) == set(CANONICAL_FIELDS)
    assert events[0]["event_type"] == "purchase"
    assert events[0]["price"] == 12.5
