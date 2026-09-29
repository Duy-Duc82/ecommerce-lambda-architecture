from speed_layer.marketplace_speed_layer import SPEED_OUTPUT_SCHEMA, checkpoint_path, speed_output_schema


def test_speed_contract_is_explicit_and_versioned():
    assert speed_output_schema() == SPEED_OUTPUT_SCHEMA
    assert checkpoint_path().endswith("marketplace_speed\\v1") or checkpoint_path().endswith("marketplace_speed/v1")
