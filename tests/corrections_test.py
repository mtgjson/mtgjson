import json
from typing import List

from mtgjson4 import mtg_corrections, mtg_builder, mtg_storage
from mtgjson4.mtg_global import CardDescription


def test_validate_corrections() -> None:
    """
    Iterate through all of the set corrections, and make sure there are no invalid actions
    """
    all_sets = mtg_builder.determine_gatherer_sets({'all_sets': True})
    assert all_sets
    empty_card_list: List[CardDescription] = []
    for set_info in all_sets:
        with mtg_storage.open_set_config_json(set_info[1], 'r') as f:
            blob = json.load(f)
            if blob.get('SET_CORRECTIONS'):
                mtg_corrections.apply_corrections(blob['SET_CORRECTIONS'], empty_card_list)


def test_fix_flavor_newline():
    cards = [
        CardDescription({
            "flavor":
            "\"The humans are useful in their way, but they must be commanded as the builder commands the stone. Be soft with them, and they will become soft.\"—Radiant, archangel",
            "multiverseid":
            5707,
            "name":
            "Serra Zealot",
            "number":
            "46",
        })
    ]

    corrections = [{
        "match": {
            "name": "Serra Zealot"
        },
        "fixFlavorNewlines": True,
    }]
    mtg_corrections.apply_corrections(corrections, cards)
    assert cards[0][
        'flavor'] == "\"The humans are useful in their way, but they must be commanded as the builder commands the stone. Be soft with them, and they will become soft.\"\n—Radiant, archangel"


def test_flavor_add_dash():
    cards = [
        CardDescription({
            "multiverseid":
            3503,
            "name":
            "Mtenda Griffin",
            "flavor":
            "\"Unlike Zhalfir, the griffin needs no council to keep harmony among its parts.\"Asmira, Holy Avenger",
        })
    ]

    corrections = [{"match": {"name": ["Ersatz Gnomes", "Mtenda Griffin"]}, "flavorAddDash": True}]

    mtg_corrections.apply_corrections(corrections, cards)
    assert cards[0][
        'flavor'] == "\"Unlike Zhalfir, the griffin needs no council to keep harmony among its parts.\"\n—Asmira, Holy Avenger"


def test_flavor_add_exclaimation():
    cards = [
        CardDescription({
            "multiverseid":
            3440,
            "name":
            "Dwarven Miner",
            "flavor":
            "\"Fetch the pestridder, Paka—we've got dwarves in the rutabagas\"\n—Jamul, Femeref farmer",
        })
    ]

    corrections = [{
        "match": {
            "name": [
                "Cerulean Wyvern", "Dwarven Miner", "Ekundu Cyclops", "Ether Well", "Floodgate", "Goblin Scouts",
                "Infernal Contract"
            ]
        },
        "flavorAddExclamation": True
    }]

    mtg_corrections.apply_corrections(corrections, cards)
    assert cards[0][
        'flavor'] == "\"Fetch the pestridder, Paka—we've got dwarves in the rutabagas!\"\n—Jamul, Femeref farmer"
