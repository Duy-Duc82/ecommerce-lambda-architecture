import unittest

from config.schema import CANONICAL_FIELDS, normalize_event


class IngestionContractTest(unittest.TestCase):
    def test_kaggle_mapping_preserves_only_source_facts(self):
        event = normalize_event(
            {
                "event_time": "2019-10-01 00:00:00 UTC",
                "event_type": "purchase",
                "product_id": "1001",
                "category_id": "10",
                "category_code": "electronics.smartphone",
                "brand": "samsung",
                "price": "500.00",
                "user_id": "501",
                "user_session": "session-1",
            }
        )
        self.assertEqual(set(event.keys()), set(CANONICAL_FIELDS))
        self.assertEqual(event["user_session"], "session-1")
        self.assertEqual(event["product_id"], "1001")
        self.assertNotIn("order_id", event)
        self.assertNotIn("payment_method", event)


if __name__ == "__main__":
    unittest.main()
