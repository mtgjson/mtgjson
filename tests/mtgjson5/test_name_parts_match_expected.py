import pytest

import mtgjson5.providers
import mtgjson5.set_builder


@pytest.mark.parametrize(
	"card_name, scryfall_uuid",
	[
		["Alrund, God of the Cosmos", "5d131784-c1a3-463e-a37b-b720af67ab62"],
		["Alrund, God of the Cosmos", "b751cf69-0a02-4cd2-abd4-cdb65ca620a8"],
		["A-Alrund, God of the Cosmos", "b443504e-1b25-4565-bad7-2575826c7bb9"],
		["Zndrsplt, Eye of Wisdom", "d5dfd236-b1da-4552-b94f-ebf6bb9dafdf"],
		["Jace, Vryn's Prodigy", "02d6d693-f1f3-4317-bcc0-c21fa8490d38"],
	],
)
def test(card_name, scryfall_uuid):
	scryfall_data = mtgjson5.providers.scryfall.monolith.ScryfallProvider().download(
		f"https://api.scryfall.com/cards/{scryfall_uuid}",
		{"format": "json"},
	)

	mtgjson_cards = mtgjson5.set_builder.build_mtgjson_card(scryfall_data)

	for mtgjson_card in mtgjson_cards:
		assert mtgjson_card.name == " // ".join(mtgjson_card.get_names())
