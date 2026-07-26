"""Crawler layer — per-site product/price snapshots (new ingestion source).

Separate from data_ingestion/: a crawler observes public catalog/price state,
not real user behavior, so it speaks config.schema's price-snapshot contract
rather than the view/cart/purchase behavioral contract.
"""
