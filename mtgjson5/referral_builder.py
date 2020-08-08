"""
Referral Map builder operations
"""
import logging
import re
from typing import List, Tuple

from .classes import MtgjsonSetObject
from .consts import OUTPUT_PATH

LOGGER = logging.getLogger(__name__)


def build_and_write_referral_map(mtgjson_set: MtgjsonSetObject) -> None:
    """
    Construct and then output the referral map
    :param mtgjson_set: MTGJSON Set
    """
    referral_map = build_referral_map(mtgjson_set)
    write_referral_map(referral_map)


def build_referral_map(mtgjson_set: MtgjsonSetObject) -> List[Tuple[str, str]]:
    """
    Construct the referral map contents
    :param mtgjson_set: MTGJSON Set
    :return: Referral content to dump
    """
    return_list = []
    string_regex = re.compile(re.escape("scryfall"), re.IGNORECASE)
    for mtgjson_card_object in mtgjson_set.cards:
        for service, url in mtgjson_card_object.purchase_urls.to_json().items():
            if service not in mtgjson_card_object.raw_purchase_urls:
                LOGGER.info(
                    f"Service {service} not found for {mtgjson_card_object.name}"
                )
                continue

            return_list.append(
                (
                    url.split("/")[-1],
                    string_regex.sub(
                        "mtgjson", mtgjson_card_object.raw_purchase_urls[service]
                    ),
                )
            )

    return return_list


def write_referral_map(single_set_referral_map: List[Tuple[str, str]]) -> None:
    """
    Dump referral map content to the database
    :param single_set_referral_map: Referrals to dump
    """
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.joinpath("ReferralMap.json").open("a", encoding="utf-8") as file:
        for entry in single_set_referral_map:
            file.write(f"/links/{entry[0]}\t{entry[1]};\n")


def fixup_referral_map() -> None:
    """
    Sort and uniquify the referral map for proper Nginx support
    """
    with OUTPUT_PATH.joinpath("ReferralMap.json").open(encoding="utf-8") as file:
        lines = list(set(file.readlines()))
        lines = sorted(lines)

    with OUTPUT_PATH.joinpath("ReferralMap.json").open("w", encoding="utf-8") as file:
        file.writelines(lines)
