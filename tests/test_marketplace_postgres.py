from batch_layer.marketplace_postgres import DATASETS, DATASET_COLUMNS, MarketplaceCachePublisher


def test_all_nine_cache_datasets_have_explicit_columns():
    assert len(DATASETS) == 9
    assert set(DATASETS) == set(DATASET_COLUMNS)
    assert all(DATASET_COLUMNS[name] for name in DATASETS)
