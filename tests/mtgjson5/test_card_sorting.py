import random

from mtgjson5.classes.mtgjson_card import MtgjsonCardObject


def test_card_sorting():
	correct_order = [
		("0", None),
		("00", None),
		("ap0a", None),
		("gn0a", None),
		("ml0b", None),
		("mlp0a", None),
		("00a", None),
		("1", None),
		("2", None),
		("2a", "a"),
		("2b", "b"),
		("3", None),
		("10", None),
		("10a", "a"),
		("10b", "b"),
		("11", None),
		("20", None),
		("", None),
	]

	test_group = []
	for number, side in correct_order:
		card = MtgjsonCardObject()
		card.number = number
		card.side = side
		test_group.append(card)

	for _ in range(0, 500):
		random.shuffle(test_group)
		test_group.sort()

		test_group_order = [(x.number, x.side) for x in test_group]

		assert correct_order == test_group_order
