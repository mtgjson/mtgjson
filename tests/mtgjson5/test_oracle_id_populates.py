import pytest

import mtgjson5.providers
import mtgjson5.set_builder


@pytest.mark.parametrize(
	"card_name, scryfall_uuid, scryfall_oracle_id",
	[
		[
			"Okaun, Eye of Chaos // Okaun, Eye of Chaos",
			"94eea6e3-20bc-4dab-90ba-3113c120fb90",
			"7dec219b-96d1-4d4c-b3ac-15eda9d2ccc6",
		],
		[
			"Phelddagrif",
			"d9631cb2-d53b-4401-b53b-29d27bdefc44",
			"d0e99535-0ea0-4aa5-99f5-6c5255b27c38",
		],
	],
)
def test(card_name, scryfall_uuid, scryfall_oracle_id):
	scryfall_data = mtgjson5.providers.scryfall.monolith.ScryfallProvider().download(
		f"https://api.scryfall.com/cards/{scryfall_uuid}",
		{"format": "json"},
	)

	mtgjson_cards = mtgjson5.set_builder.build_mtgjson_card(scryfall_data)

	for mtgjson_card in mtgjson_cards:
		assert mtgjson_card.identifiers.scryfall_oracle_id == scryfall_oracle_id
