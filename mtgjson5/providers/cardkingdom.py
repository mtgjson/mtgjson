"""
Card Kingdom 3rd party provider
"""
import logging
import pathlib
from typing import Any, Dict, Optional, Union

from singleton_decorator import singleton

from ..classes import MtgjsonPricesObject
from ..providers.abstract import AbstractProvider
from ..utils import generate_card_mapping, retryable_session

LOGGER = logging.getLogger(__name__)


@singleton
class CardKingdomProvider(AbstractProvider):
    """
    Card Kingdom container
    """

    api_url: str = "https://api.cardkingdom.com/api/pricelist"

    def __init__(self) -> None:
        """
        Initializer
        """
        super().__init__(self._build_http_header())

    def _build_http_header(self) -> Dict[str, str]:
        """
        Construct the Authorization header for CardHoarder
        :return: Authorization header
        """
        return {}

    def download(
        self, url: str, params: Optional[Dict[str, Union[str, int]]] = None
    ) -> Any:
        """
        Download content
        Api calls always return JSON
        :param url: URL to download from
        :param params: Options for URL download
        """
        session = retryable_session()
        session.headers.update(self.session_header)

        response = session.get(url)
        self.log_download(response)

        return response.json()

    def generate_today_price_dict(
        self, all_printings_path: pathlib.Path
    ) -> Dict[str, MtgjsonPricesObject]:
        """
        Generate a single-day price structure for MTGO from CardHoarder
        :return MTGJSON prices single day structure
        """
        request_api_response: Dict[str, Any] = self.download(self.api_url)
        price_data_rows = request_api_response.get("data", [])

        # Start with non-foil IDs
        card_kingdom_id_to_mtgjson = generate_card_mapping(
            all_printings_path, ("identifiers", "cardKingdomId"), ("uuid",)
        )

        # Then add in foil IDs
        card_kingdom_id_to_mtgjson.update(
            generate_card_mapping(
                all_printings_path, ("identifiers", "cardKingdomFoilId"), ("uuid",)
            )
        )

        default_prices_obj = MtgjsonPricesObject(
            "paper", "cardkingdom", self.today_date, "USD"
        )

        LOGGER.info("Building CardKingdom buylist & retail data")
        return super().generic_generate_today_price_dict(
            third_party_to_mtgjson=card_kingdom_id_to_mtgjson,
            price_data_rows=price_data_rows,
            card_platform_id_key="id",
            default_prices_object=default_prices_obj,
            foil_key="is_foil",
            retail_key="price_retail",
            buy_key="price_buy",
        )


@singleton
class CardKingdomSealedProvider(AbstractProvider):
	"""
	Separate provider for Card Kingdom sealed products
	"""
	
	api_url: str = "https://api.cardkingdom.com/api/sealed_pricelist"
	
	def __init__(self) -> None:
        """
        Initializer
        """
        super().__init__(self._build_http_header())

    def _build_http_header(self) -> Dict[str, str]:
        """
        Construct the Authorization header
        :return: Authorization header
        """
        return {}

	def determine_mtgjson_sealed_product_category(
        self, product_name: str
    ) -> MtgjsonSealedProductCategory:
        """
        Best-effort to parse the product name and determine the sealed product category
        :param product_name Name of the product from TCG, must be lowercase
        :return: category
        """

        # The order of the checks is important: for example, we need to catch all 'case'
        # products before everything else, since "booster box case" would be caught in
        # the 'booster box' check. The same applies to several sub products, such as
        # 'prerelease' (vs guild kit), 'box set' (vs set boosters), and so on.
        if any(
            tag in product_name
            for tag in [
                "booster case",
                "box case",
                "bundle case",
                "display case",
                "intro display",
                "intro pack display",
                "pack box",
                "pack case",
                "tournament pack display",
                "vip edition box",
            ]
        ):
            return MtgjsonSealedProductCategory.CASE

        if any(
            tag in product_name
            for tag in ["booster box", "booster display", "mythic edition"]
        ):
            return MtgjsonSealedProductCategory.BOOSTER_BOX

        if any(
            tag in product_name
            for tag in [
                "booster pack",
                "booster retail pack",
                "box topper",
                "hanger pack",
                "omega pack",
                "promo pack",
                "theme booster",
                "vip edition pack",
            ]
        ):
            if "set of" in product_name:
                return MtgjsonSealedProductCategory.SUBSET
            return MtgjsonSealedProductCategory.BOOSTER_PACK

        if "prerelease" in product_name:
            return MtgjsonSealedProductCategory.PRERELEASE_PACK

        if any(
            tag in product_name
            for tag in [
                "blister pack",
                "draft pack",
                "draft set",
                "multipack",
            ]
        ):
            return MtgjsonSealedProductCategory.DRAFT_SET

        if any(
            tag in product_name
            for tag in [
                "booster battle pack",
                "clash pack",
                "fourth edition gift box",
                "quick start set",
                "two player starter",
            ]
        ):
            return MtgjsonSealedProductCategory.TWO_PLAYER_STARTER_SET

        if any(
            tag in product_name
            for tag in [
                "box set",
                "builders toolkit",
                "commander collection",
                "deckmasters tin",
                "deluxe collection",
                "edition box",
                "game night",
                "global series",
                "hascon",
                "modern event deck",
                "planechase",
                "planeswalker set",
                "premium deck series",
                "sdcc",
                "secret lair",
                "signature spellbook",
            ]
        ):
            # In this section, only Planechase may have a "set of"
            if "planechase" in product_name and "set of" in product_name:
                return MtgjsonSealedProductCategory.CASE
            return MtgjsonSealedProductCategory.BOX_SET

        if "commander" in product_name or "brawl deck" in product_name:
            if "set of" in product_name:
                return MtgjsonSealedProductCategory.CASE
            return MtgjsonSealedProductCategory.COMMANDER_DECK

        if "deck box" in product_name or "deck display" in product_name:
            return MtgjsonSealedProductCategory.DECK_BOX

        if any(
            tag in product_name
            for tag in [
                "challenge deck",
                "challenger deck",
                "championship deck",
                "event deck",
                "guild kit",
                "intro pack",
                "planeswalker deck",
                "starter deck",
                "theme deck",
                "tournament deck",
                "tournament pack",
            ]
        ):
            if "set of" in product_name:
                return MtgjsonSealedProductCategory.SUBSET
            return MtgjsonSealedProductCategory.DECK

        if any(
            tag in product_name
            for tag in [
                "bundle",
                "fat pack",
                "gift box",
            ]
        ):
            return MtgjsonSealedProductCategory.BUNDLE

        if "land station" in product_name:
            return MtgjsonSealedProductCategory.LAND_STATION

        return MtgjsonSealedProductCategory.UNKNOWN

    # Best-effort to parse the product name and determine the sealed product category
    def determine_mtgjson_sealed_product_subtype(
        self, product_name: str, category: MtgjsonSealedProductCategory
    ) -> MtgjsonSealedProductSubtype:
        """
        Best-effort to parse the product name and determine the sealed product subtype
        :param product_name Name of the product from TCG
        :param category Category as parsed from determine_mtgjson_sealed_product_category()
        :return: subtype
        """
        if category == MtgjsonSealedProductCategory.UNKNOWN:
            return MtgjsonSealedProductSubtype.UNKNOWN

        for subtype in MtgjsonSealedProductSubtype:
            if subtype is MtgjsonSealedProductSubtype.UNKNOWN:
                continue

            # Prevent aliasing from Eventide
            if (
                subtype is MtgjsonSealedProductSubtype.EVENT
                and category is not MtgjsonSealedProductCategory.DECK
            ):
                continue

            # Prevent assigning 'set' (for set boosters) to unrelated categories
            if subtype is MtgjsonSealedProductSubtype.SET and (
                category is not MtgjsonSealedProductCategory.BOOSTER_PACK
                and category is not MtgjsonSealedProductCategory.BOOSTER_BOX
            ):
                continue

            # Do the replace to use the tag as text
            if subtype.value.replace("_", " ") in product_name:
                return subtype

        # Special handling because sometimes 'default' is not tagged
        if category in [
            MtgjsonSealedProductCategory.BOOSTER_BOX,
            MtgjsonSealedProductCategory.BOOSTER_PACK,
        ]:
            return MtgjsonSealedProductSubtype.DEFAULT
        return MtgjsonSealedProductSubtype.UNKNOWN

	def get_cardkingdom_sealed_data(self):
		"""
		Download data from url
		"""
		session = retryable_session()

        response = session.get(self.api_url)
        self.log_download(response)
        if response.ok:
            return response.json()

        LOGGER.error(
            f"Error downloading GitHub Boosters: {response} --- {response.text}"
        )
        return {}

    def update_sealed_product(
        self, set_name: str, sealed_products: List[MtgjsonSealedProductObject]
    ) -> List[MtgjsonSealedProductObject]:
        """
        Builds MTGJSON Sealed Product Objects from Card Kingdom data
        :param group_id: group id for the set to get data for
        :param set_code: short abbreviation for the set name
        :return: A list of MtgjsonSealedProductObject for a given set
        """
        if not self.__keys_found:
            LOGGER.warning("Keys not found for Card Kingdom, skipping")
            return []

        sealed_data = get_cardkingdom_sealed_data()

        mtgjson_sealed_products = []

        with constants.RESOURCE_PATH.joinpath("sealed_name_fixes.json").open(
            encoding="utf-8"
        ) as f:
            sealed_name_fixes = json.load(f)

        with constants.RESOURCE_PATH.joinpath("booster_box_size_overrides.json").open(
            encoding="utf-8"
        ) as f:
            booster_box_size_overrides = json.load(f)
            
        existing_names = set([product.name for product in sealed_products])

        for product in sealed_data:
        	if product["edition"] != set_name:
        		continue
        	
        	if product["name"] in existing_names:
        		LOGGER.debug(
        			f"{sealed_product.name}: adding CardKingdom values"
        		)
        		sealed_product = next(
        			p for p in sealed_products if p.name == product['name']
        		)
        		sealed_product.raw_purchase_urls["cardKingdom"] = product["url"]
        		sealed_product.identifiers.card_kingdom_id = str(product["id"])
        		continue
        	
            sealed_product = MtgjsonSealedProductObject()

            sealed_product.name = product["name"]
            for tag, fix in sealed_name_fixes.items():
                if tag in sealed_product.name:
                    sealed_product.name = sealed_product.name.replace(tag, fix)

            sealed_product.identifiers.card_kingdom_id = str(product["id"])

            sealed_product.category = self.determine_mtgjson_sealed_product_category(
                sealed_product.name.lower()
            )
            sealed_product.subtype = self.determine_mtgjson_sealed_product_subtype(
                sealed_product.name.lower(), sealed_product.category
            )

            LOGGER.debug(
                f"{sealed_product.name}: {sealed_product.category.value}.{sealed_product.subtype.value}"
            )

            if sealed_product.category == MtgjsonSealedProductCategory.BOOSTER_BOX:
                sealed_product.product_size = int(
                    booster_box_size_overrides.get(
                        sealed_product.subtype.value, {}
                    ).get(
                        set_code,
                        self.product_default_size.get(sealed_product.subtype.value, 0),
                    )
                )

            if sealed_product.release_date is not None:
                sealed_product.release_date = sealed_product.release_date[0:10]
            sealed_product.raw_purchase_urls["cardKingdom"] = product["url"]
            mtgjson_sealed_products.append(sealed_product)

        sealed_products.extend(mtgjson_sealed_products)
