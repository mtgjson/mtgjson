import random

from mtgjson5.classes.mtgjson_card import MtgjsonCardObject


def test_card_sorting():
    correct_order = [
        ("A", "1", None),
        ("B", "2", None),
        ("C1", "2a", "a"),
        ("C2", "2b", "b"),
        ("D", "3", None),
        ("E", "10", None),
        ("F1", "10a", "a"),
        ("F2", "10b", "b"),
        ("G", "11", None),
        ("H", "20", None),
        ("I", "", None),
    ]

    test_group = []
    for name, number, side in correct_order:
        card = MtgjsonCardObject()
        card.name = name
        card.number = number
        card.side = side
        test_group.append(card)

    random.shuffle(test_group)
    test_group.sort()

    test_group_order = list(map(lambda x: (x.name, x.number, x.side), test_group))

    assert correct_order == test_group_order
