import json
from typing import List
from mtgjson4 import corrections, mtg_builder, mtg_storage
from mtgjson4.mtg_global import CardDescription


def test_validate_corrections() -> None:
    all_sets = mtg_builder.determine_gatherer_sets({'all_sets': True})
    assert all_sets
    empty_card_list: List[CardDescription] = []
    for set_info in all_sets:
        with mtg_storage.open_set_config_json(set_info[1], 'r') as f:
            blob = json.load(f)
            if blob.get('SET_CORRECTIONS'):
                corrections.apply_corrections(blob['SET_CORRECTIONS'], empty_card_list)
