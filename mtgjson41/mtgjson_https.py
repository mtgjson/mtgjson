# This package manages all external web connections
import json
from typing import List, Dict, Any, Tuple

import requests

SUPERTYPES: List[str] = [
    'Basic',
    'Legendary',
    'Ongoing',
    'Snow',
    'World',
]

SCRYFALL_API_SETS = "https://api.scryfall.com/sets/"


def get_cards_from_scryfall(set_code: str) -> List[Dict[str, Any]]:
    """
    Connects to Scryfall API and goes through all redirects to get the
    card data from their several pages via multiple API calls.
    :param set_code: Set to download (Ex: AER, M19)
    :return: List of all card objects
    """
    # Access set page to find search URLs
    set_api_json: json = requests.get(SCRYFALL_API_SETS + set_code).json()
    cards_api_url: str = set_api_json["search_uri"]

    # All cards in the set structure
    scryfall_cards: List[Dict[str, Any]] = list()

    # For each page, append all the data, go to next page
    while cards_api_url is not None:
        cards_api_json: json = requests.get(cards_api_url).json()

        for card in cards_api_json["data"]:
            scryfall_cards.append(card)

        if cards_api_json["has_more"]:
            cards_api_url = cards_api_json["next_page"]
        else:
            cards_api_url = None

    return scryfall_cards


def parse_from_scryfall_cards(sf_cards: List[Dict[str, Any]], sf_card_face: int = 0) -> List[Dict[str, Any]]:
    mtgjson_cards: List[Dict[str, Any]] = list()

    for sf_card in sf_cards:
        mtgjson_card: Dict[str, Any] = dict()

        print("Parsing", sf_card.get("name"))

        # If flip-type, go to card_faces for alt attributes
        face_data: Dict[str, Any] = sf_card
        if "card_faces" in sf_card:
            mtgjson_card["names"]: List[str] = sf_card.get("name").split(" // ")
            face_data = sf_card.get("card_faces")[sf_card_face]

            # Recursively parse the other cards within this card too
            # Only call recursive if it is the first time we see this card object
            if sf_card_face == 0:
                for i in range(1, len(sf_card.get("card_faces"))):
                    mtgjson_cards += parse_from_scryfall_cards([sf_card], i)

        # Characteristics that can are not shared to both sides of flip-type cards
        mtgjson_card["manaCost"]: str = face_data.get("mana_cost")
        mtgjson_card["name"]: str = face_data.get("name")
        mtgjson_card["type"]: str = face_data.get("type_line")
        mtgjson_card["text"]: str = face_data.get("oracle_text")
        mtgjson_card["colors"]: List[str] = face_data.get("colors")
        mtgjson_card["power"]: str = face_data.get("power")
        mtgjson_card["toughness"]: str = face_data.get("toughness")
        mtgjson_card["loyalty"]: str = face_data.get("loyalty")
        mtgjson_card["watermark"]: str = face_data.get("watermark")
        mtgjson_card["multiverseid"]: int = sf_card["multiverse_ids"][sf_card_face]

        # Characteristics that are shared to all sides of flip-type cards, that we don't have to modify
        mtgjson_card["artist"]: str = sf_card.get("artist")
        mtgjson_card["borderColor"]: str = sf_card.get("border_color")
        mtgjson_card["colorIdentity"]: List[str] = sf_card.get("color_identity")
        mtgjson_card["convertedManaCost"]: float = sf_card.get("cmc")
        mtgjson_card["flavorText"]: str = sf_card.get("flavor_text")
        mtgjson_card["frameVersion"]: str = sf_card.get("frame")
        mtgjson_card["hasFoil"]: bool = sf_card.get("foil")
        mtgjson_card["hasNonFoil"]: bool = sf_card.get("nonfoil")
        mtgjson_card["isOnlineOnly"]: bool = sf_card.get("digital")
        mtgjson_card["isOversized"]: bool = sf_card.get("oversized")
        mtgjson_card["layout"]: str = sf_card.get("layout")
        mtgjson_card["number"]: str = sf_card.get("collector_number")
        mtgjson_card["reserved"]: bool = sf_card.get("reserved")
        mtgjson_card["uuid"]: str = sf_card.get("id")

        # Characteristics that we have to format ourselves from provided data
        mtgjson_card["timeshifted"]: bool = (sf_card.get("timeshifted") or sf_card.get("futureshifted"))
        mtgjson_card["rarity"]: str = sf_card.get("rarity") if not mtgjson_card.get("timeshifted") else "Special"

        # Characteristics that we need custom functions to parse
        mtgjson_card["legalities"] = parse_scryfall_legalities(sf_card["legalities"])
        mtgjson_card["supertypes"], mtgjson_card["types"], mtgjson_card["subtypes"] = parse_scryfall_card_types(mtgjson_card["type"])
        mtgjson_card["rulings"] = parse_scryfall_rulings(sf_card["rulings_uri"])


        """  
        # Characteristics we have to do further API calls for
        mtgjson_card["foreignData"] = API_CALL(sf_card[""])
        mtgjson_card["rulings"] = API_CALL(sf_card["rulings_uri"])
        # mtgjson_card["originalText"] = ""
        # mtgjson_card["originalType"] = ""
        # mtgjson_card["printings"] = UGH()
        
        """
        mtgjson_cards.append(mtgjson_card)
    return mtgjson_cards


def parse_scryfall_legalities(sf_card_legalities: Dict[str, str]) -> Dict[str, str]:
    # Create the legalities field
    card_legalities: Dict[str, str] = dict()
    for key, value in sf_card_legalities.items():
        if value is not "not_legal":
            card_legalities[key] = value.replace("banned", "Banned").replace("legal", "Legal").replace("restricted", "Restricted")

    return card_legalities


# TODO: Refactor!!
def parse_scryfall_card_types(card_type: str) -> Tuple[List[str], List[str], List[str]]:
    sub_types: List[str] = list()
    super_types: List[str] = list()
    types: List[str] = list()

    """
    Legendary Snow Artifact Creature -- Brick House
    super = [Legendary, Snow]
    types = [Artifact, Creature]
    sub = [Brick, House]
    """

    if '—' in card_type:
        supertypes_and_types, subtypes = card_type.split('—')
        sub_types = subtypes.split(' ')
    else:
        supertypes_and_types = types

    for value in str(supertypes_and_types).split(' '):
        if value in SUPERTYPES:
            super_types.append(value)
        else:
            types.append(value)

    return super_types, types, sub_types


def parse_scryfall_rulings(rulings_url: str) -> List[Dict[str, str]]:
    rules_api_json: json = requests.get(rulings_url).json()

    sf_rules: List[Dict[str, str]] = list()
    mtgjson_rules: List[Dict[str, str]] = list()

    for rule in rules_api_json["data"]:
        sf_rules.append(rule)

    for sf_rule in sf_rules:
        mtgjson_rule: Dict[str, str] = dict()
        mtgjson_rule["date"] = sf_rule["published_at"]
        mtgjson_rule["text"] = sf_rule["comment"]
        mtgjson_rules.append(mtgjson_rule)

    return mtgjson_rules


def main():
    cards = get_cards_from_scryfall("isd")
    print(json.dumps(parse_from_scryfall_cards(cards)))


if __name__ == '__main__':
    print("Calling main")
    main()
    print("Ending  main")
