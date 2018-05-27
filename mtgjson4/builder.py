import ast
import asyncio
import contextlib
import hashlib
import json
import os
import pathlib
import re
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import aiohttp
import bs4

from mtgjson4.download import (generate_mids_by_set, get_card_details,
                               get_card_foreign_details, get_card_legalities,
                               get_checklist_urls)
from mtgjson4.globals import (CARD_TYPES, COLORS, RESERVE_LIST, SUPERTYPES,
                              Color, get_language_long_name)
from mtgjson4.parsing import (replace_symbol_images_with_tokens)
from mtgjson4.storage import (is_set_file, open_set_json, open_set_config_json)

from mtgjson4.storage import (SET_CONFIG_DIR)

class MtgJson:

    def __init__(self,
                 sets_to_build: List[List[str]],
                 session: Optional[aiohttp.ClientSession] = None,
                 loop: Optional[asyncio.AbstractEventLoop] = None
                 ) -> None:
        """
        Start the class and define the i/o session and sets we'll have to build
        :param sets_to_build:
        :param session:
        :param loop:
        """
        if loop is None:
            loop = asyncio.events.get_event_loop()
        if session is None:
            session = aiohttp.ClientSession(loop=loop, raise_for_status=True, conn_timeout=None, read_timeout=None)
        self.loop = loop
        self.http_session = session
        self.sets_to_build = sets_to_build

    async def get_card_html(self,
                            card_mid: int,
                            is_printed: bool=False
                            ) -> bs4.BeautifulSoup:
        """
        Gets the card details (first page) of a single card
        :param card_mid:
        :param is_printed:
        :return:
        """
        html = await get_card_details(self.http_session, card_mid, is_printed)
        soup = bs4.BeautifulSoup(html, 'html.parser')
        return soup

    # TODO: Fix adding another card
    @staticmethod
    async def determine_layout_and_div_name(soup: bs4.BeautifulSoup,
                                            is_second_card: bool,
                                            ) -> Tuple[str, str, Optional[bool]]:
        # Determine how many cards on on the page
        cards_total = len(soup.select('table[class^=cardDetails]'))

        # Values to return
        div_name = 'ctl00_ctl00_ctl00_MainContent_SubContent_SubContent_{}'
        layout = 'unknown'
        add_additional_card = False

        if cards_total == 1:
            layout = 'normal'
        elif cards_total == 2:
            layout = 'double'
            if is_second_card:
                div_name = div_name[:-3] + '_ctl03_{}'
            else:
                div_name = div_name[:-3] + '_ctl02_{}'
                add_additional_card = True

        return (layout, div_name, add_additional_card)

    @staticmethod
    async def parse_card_name(soup: bs4.BeautifulSoup,
                              parse_div: str
                              ) -> str:
        """
        Parse the card name from the row
        :param soup:
        :param parse_div:
        :return: card name from MID
        """
        name_row = soup.find(id=parse_div.format('nameRow'))
        name_row = name_row.findAll('div')[-1]
        card_name = name_row.get_text(strip=True)

        return card_name

    @staticmethod
    async def parse_card_cmc(soup: bs4.BeautifulSoup,
                             parse_div: str
                             ) -> Union[int, float]:
        """
        Parse the card CMC from the row
        :param soup:
        :param parse_div:
        :return: cmc from MID
        """
        cmc_row = soup.find(id=parse_div.format('cmcRow'))

        if cmc_row is None:
            return 0

        cmc_row = cmc_row.findAll('div')[-1]
        card_cmc = cmc_row.get_text(strip=True)

        try:
            card_cmc = int(card_cmc)
        except ValueError:  # Little Girl causes this, for example
            card_cmc = float(card_cmc)

        return card_cmc

    @staticmethod
    async def parse_card_other_name(soup: bs4.BeautifulSoup,
                                    parse_div: str,
                                    layout: str
                                    ) -> List[Union[bool, Optional[str]]]:
        """
        If the MID has 2 cards, return the other card's name
        :param soup:
        :param parse_div:
        :param layout:
        :return: If the layout matches, return the other card's name
        """
        if layout == 'double':
            if 'ctl02' in parse_div:
                other_div_name = parse_div.replace('02', '03')
            else:
                other_div_name = parse_div.replace('03', '02')

            other_name_row = soup.find(id=other_div_name.format('nameRow'))
            other_name_row = other_name_row.findAll('div')[-1]
            card_other_name = other_name_row.get_text(strip=True)

            return [True, card_other_name]

        return [False, None]

    @staticmethod
    async def parse_card_types(soup: bs4.BeautifulSoup,
                               parse_div: str
                               ) -> List[Union[List[str], List[str], List[str], str]]:
        """
        Parse the types of the card and split them into 4 different structures
        super types, normal types, sub types, and the full row (all the types)
        :param soup:
        :param parse_div:
        :return:
        """
        card_super_types: List[str] = []
        card_types: List[str] = []
        card_sub_types: List[str] = []

        type_row = soup.find(id=parse_div.format('typeRow'))
        type_row = type_row.findAll('div')[-1]
        type_row = type_row.get_text(strip=True).replace('  ', ' ')

        if '—' in type_row:
            supertypes_and_types, subtypes = type_row.split('—')
            card_sub_types = subtypes.split()
        else:
            supertypes_and_types = type_row

        for value in supertypes_and_types.split():
            if value in SUPERTYPES:
                card_super_types.append(value)
            elif value in CARD_TYPES:
                card_types.append(value)
            else:
                card_types.append(value)
                # raise ValueError(f'Unknown supertype or card type: {value}')

        return [card_super_types, card_types, card_sub_types, type_row]

    @staticmethod
    async def parse_colors_and_cost(soup: bs4.BeautifulSoup,
                                    parse_div: str
                                    ) -> Tuple[Optional[List[Color]], Optional[str]]:
        """
        Parse the colors and mana cost of the card
        Can use the colors to build the color identity later
        :param soup:
        :param parse_div:
        :return:
        """
        mana_row = soup.find(id=parse_div.format('manaRow'))
        if mana_row:
            mana_row = mana_row.findAll('div')[-1]

            card_colors: Set[Color] = set()

            mana_colors = replace_symbol_images_with_tokens(mana_row)[1]
            card_cost = mana_row.get_text(strip=True)
            card_colors.update(mana_colors)

            # Sort field in WUBRG order
            sorted_colors = sorted(
                list(filter(lambda c: c in card_colors, COLORS)),
                key=lambda word: [COLORS.index(Color(c)) for c in word]
            )

            return (sorted_colors, card_cost)

        return (None, None)

    @staticmethod
    async def parse_card_text_and_color_identity(soup: bs4.BeautifulSoup, parse_div: str,
                                                 card_colors: Optional[List[Color]]
                                                 ) -> Tuple[Optional[str], List[Color]]:
        text_row = soup.find(id=parse_div.format('textRow'))
        return_text = ''
        return_color_identity = set()

        if card_colors:
            return_color_identity.update(card_colors)

        if text_row is not None:
            text_row = text_row.select('div[class^=cardtextbox]')

            return_text = ''
            for div in text_row:
                # Start by replacing all images with alternative text
                div, instance_color_identity = replace_symbol_images_with_tokens(div)

                return_color_identity.update(instance_color_identity)

                # Next, just add the card text, line by line
                return_text += div.get_text() + '\n'

            return_text = return_text.strip()  # Remove last '\n'

        # Sort field in WUBRG order
        sorted_color_identity = sorted(
            list(filter(lambda c: c in return_color_identity, COLORS)),
            key=lambda word: [COLORS.index(Color(c)) for c in word]
        )

        return (return_text or None, sorted_color_identity)

    @staticmethod
    async def parse_card_flavor(soup: bs4.BeautifulSoup,
                                parse_div: str
                                ) -> Optional[str]:
        flavor_row = soup.find(id=parse_div.format('flavorRow'))
        card_flavor_text = ''
        if flavor_row is not None:
            flavor_row = flavor_row.select('div[class^=flavortextbox]')

            for div in flavor_row:
                card_flavor_text += div.get_text() + '\n'

            card_flavor_text = card_flavor_text.strip() # Remove last '\n'

        if not card_flavor_text:
            return None
        return card_flavor_text

    @staticmethod
    async def parse_card_pt_loyalty_vanguard(soup: bs4.BeautifulSoup,
                                             parse_div: str
                                             ) -> List[Union[Optional[str],
                                                             Optional[str],
                                                             Optional[str],
                                                             Optional[str],
                                                             Optional[str]]]:
        pt_row = soup.find(id=parse_div.format('ptRow'))

        power = None
        toughness = None
        loyalty = None
        hand = None
        life = None

        if pt_row is not None:
            pt_row = pt_row.findAll('div')[-1]
            pt_row = pt_row.get_text(strip=True)

            # If Vanguard
            if 'Hand Modifier' in pt_row:
                pt_row = pt_row.split('\xa0,\xa0')
                hand = pt_row[0].split(' ')[-1]
                life = pt_row[1].split(' ')[-1][:-1]
            elif '/' in pt_row:
                card_power, card_toughness = pt_row.split('/')
                power = card_power.strip()
                toughness = card_toughness.strip()
            else:
                loyalty = pt_row.strip()

        return [power, toughness, loyalty, hand, life]

    @staticmethod
    async def parse_card_rarity(soup: bs4.BeautifulSoup,
                                parse_div: str
                                ) -> str:
        rarity_row = soup.find(id=parse_div.format('rarityRow'))
        rarity_row = rarity_row.findAll('div')[-1]
        card_rarity = rarity_row.find('span').get_text(strip=True)
        return card_rarity

    @staticmethod
    async def parse_card_number(soup: bs4.BeautifulSoup,
                                parse_div: str
                                ) -> Optional[str]:
        number_row = soup.find(id=parse_div.format('numberRow'))
        card_number = None
        if number_row is not None:
            number_row = number_row.findAll('div')[-1]
            card_number = number_row.get_text(strip=True)

        return card_number

    @staticmethod
    async def parse_artists(soup: bs4.BeautifulSoup,
                            parse_div: str
                            ) -> List[str]:
        with contextlib.suppress(AttributeError):  # Un-cards might not have an artist!
            artist_row = soup.find(id=parse_div.format('artistRow'))
            artist_row = artist_row.findAll('div')[-1]
            card_artists = artist_row.find('a').get_text(strip=True).split('&')

        return card_artists if card_artists else list()

    @staticmethod
    async def parse_watermark(soup: bs4.BeautifulSoup,
                              parse_div: str
                              ) -> Optional[str]:
        card_watermark = None
        watermark_row = soup.find(id=parse_div.format('markRow'))
        if watermark_row is not None:
            watermark_row = watermark_row.findAll('div')[-1]
            card_watermark = watermark_row.get_text(strip=True)

        return card_watermark

    @staticmethod
    async def parse_rulings(soup: bs4.BeautifulSoup,
                            parse_div: str
                            ) -> List[dict]:
        rulings: List[Dict[str, str]] = list()
        rulings_row = soup.find(id=parse_div.format('rulingsRow'))
        if rulings_row is not None:
            rulings_dates = rulings_row.findAll('td', id=re.compile(r'\w*_rulingDate\b'))
            rulings_text = rulings_row.findAll('td', id=re.compile(r'\w*_rulingText\b'))
            for ruling_text in rulings_text:
                ruling_text = replace_symbol_images_with_tokens(ruling_text)[0]

            rulings = [
                {
                    'date': ruling_date.get_text(),
                    'text': ruling_text.get_text()
                }
                for ruling_date, ruling_text in zip(rulings_dates, rulings_text)
            ]

        return rulings

    async def parse_card_sets(self,
                              soup: bs4.BeautifulSoup,
                              parse_div: str,
                              card_set: str
                              ) -> List[str]:
        card_printings = [card_set]
        sets_row = soup.find(id=parse_div.format('otherSetsRow'))
        if sets_row is not None:
            images = sets_row.findAll('img')

            for symbol in images:
                this_set_name = symbol['alt'].split('(')[0].strip()

                card_printings += (
                    set_code[1] for set_code in self.sets_to_build if this_set_name == set_code[0]
                )

        return card_printings

    @staticmethod
    async def parse_card_variations(soup: bs4.BeautifulSoup,
                                    parse_div: str,
                                    card_mid: int
                                    ) -> List[int]:
        card_variations = []
        variations_row = soup.find(id=parse_div.format('variationLinks'))
        if variations_row is not None:
            for variations_info in variations_row.findAll('a', {'class': 'variationLink'}):
                card_variations.append(int(variations_info['href'].split('multiverseid=')[1]))

            with contextlib.suppress(ValueError):
                card_variations.remove(card_mid)  # Don't need this card's MID in its variations

        return card_variations

    async def build_main_part(self,
                              set_name: List[str],
                              card_mid: int,
                              card_info: dict,
                              other_cards_holder: Optional[List[object]],
                              second_card: bool=False
                              ) -> None:
        # Parse web page so we can gather all data from it
        soup_oracle = await self.get_card_html(card_mid)

        card_layout, div_name, add_other_card = await self.determine_layout_and_div_name(soup_oracle, second_card)
        if add_other_card and other_cards_holder is not None:
            other_cards_holder.append(self.loop.create_task(self.build_card(set_name,
                                                                            card_mid,
                                                                            None,
                                                                            second_card=True)))

        card_info['multiverseid'] = int(card_mid)
        card_info['name'] = await self.parse_card_name(soup_oracle, div_name)
        card_info['cmc'] = await self.parse_card_cmc(soup_oracle, div_name)

        # Get other side's name for the user
        has_other, card_other_name = await self.parse_card_other_name(soup_oracle, div_name, card_layout)
        if has_other:
            card_info['names'] = [card_info['name'], card_other_name]

        # Get card's colors and mana cost
        card_colors, card_cost = await self.parse_colors_and_cost(soup_oracle, div_name)
        if card_colors:
            card_info['colors'] = card_colors
        if card_cost:
            card_info['manaCost'] = card_cost

        # Get Card Type(s)
        card_super_types, card_types, card_sub_types, full_type = await self.parse_card_types(soup_oracle, div_name)
        if card_super_types:
            card_info['supertypes'] = card_super_types
        if card_types:
            card_info['types'] = card_types
        if card_sub_types:
            card_info['subtypes'] = card_sub_types
        if full_type:
            card_info['type'] = full_type

        # Get Card Text and Color Identity
        card_info['text'], card_info['colorIdentity'] = await self.parse_card_text_and_color_identity(soup_oracle,
                                                                                                      div_name,
                                                                                                      card_colors)

        # Get Card Flavor Text
        c_flavor = await self.parse_card_flavor(soup_oracle, div_name)
        if c_flavor:
            card_info['flavor'] = c_flavor

        # Get Card P/T OR Loyalty OR Hand/Life
        c_power, c_toughness, c_loyalty, c_hand, c_life = await self.parse_card_pt_loyalty_vanguard(soup_oracle,
                                                                                                    div_name)
        if c_power:
            card_info['power'] = c_power
        if c_toughness:
            card_info['toughness'] = c_toughness
        if c_loyalty:
            card_info['loyalty'] = c_loyalty
        if c_hand:
            card_info['hand'] = c_hand
        if c_life:
            card_info['life'] = c_life

        # Get Card Rarity
        card_info['rarity'] = await self.parse_card_rarity(soup_oracle, div_name)

        # Get Card Set Number
        c_number = await self.parse_card_number(soup_oracle, div_name)
        if c_number:
            card_info['number'] = c_number

        # Get Card Artist(s)
        card_info['artist'] = await self.parse_artists(soup_oracle, div_name)

        # Get Card Watermark
        c_watermark = await self.parse_watermark(soup_oracle, div_name)
        if c_watermark:
            card_info['watermark'] = c_watermark

        # Get Card Reserve List Status
        if card_info['name'] in RESERVE_LIST:
            card_info['reserved'] = True

        # Get Card Rulings
        c_rulings = await self.parse_rulings(soup_oracle, div_name)
        if c_rulings:
            card_info['rulings'] = c_rulings

        # Get Card Sets
        card_info['printings'] = await self.parse_card_sets(soup_oracle, div_name, set_name[1])

        # Get Card Variations
        c_variations = await self.parse_card_variations(soup_oracle, div_name, card_mid)
        if c_variations:
            card_info['variations'] = c_variations

    @staticmethod
    async def parse_card_legal(soup: bs4.BeautifulSoup,
                               ) -> List[Dict[str, Any]]:
        format_rows = soup.select('table[class^=cardList]')[1]
        format_rows = format_rows.select('tr[class^=cardItem]')
        card_formats: List[Dict[str, Any]] = []
        with contextlib.suppress(IndexError):  # if no legalities, only one tr with only one td
            for div in format_rows:
                table_rows = div.findAll('td')
                card_format_name = table_rows[0].get_text(strip=True)
                card_format_legal = table_rows[1].get_text(strip=True)  # raises IndexError if no legalities

                card_formats.append({
                    'format': card_format_name,
                    'legality': card_format_legal
                })

        return card_formats

    async def build_legalities_part(self,
                                    card_mid: int,
                                    card_info: dict
                                    ) -> None:
        try:
            html = await get_card_legalities(self.http_session, card_mid)
        except aiohttp.ClientError as error:
            # If Gatherer errors, omit the data for now
            # This can be appended on a case-by-case basis
            if error.code == 500:
                return  # Page doesn't work, nothing we can do
            else:
                print("Unknown error: ", error.code)
                return

        # Parse web page so we can gather all data from it
        soup_oracle = bs4.BeautifulSoup(html, 'html.parser')

        # Get Card Legalities
        c_legal = await self.parse_card_legal(soup_oracle)
        if c_legal:
            card_info['legalities'] = c_legal

    @staticmethod
    async def parse_foreign_info(soup: bs4.BeautifulSoup) -> List[Dict[str, Any]]:
        language_rows = soup.select('table[class^=cardList]')[0]
        language_rows = language_rows.select('tr[class^=cardItem]')

        card_languages: List[Dict[str, Any]] = []
        for div in language_rows:
            table_rows = div.findAll('td')

            a_tag = table_rows[0].find('a')
            foreign_mid = a_tag['href'].split('=')[-1]
            card_language_mid = foreign_mid
            card_foreign_name_in_language = a_tag.get_text(strip=True)

            card_language_name = table_rows[1].get_text(strip=True)

            card_languages.append({
                'language': card_language_name,
                'name': card_foreign_name_in_language,
                'multiverseid': card_language_mid
            })

        return card_languages

    async def build_foreign_part(self,
                                 card_mid: int,
                                 card_info: dict
                                 ) -> None:
        try:
            html = await get_card_foreign_details(self.http_session, card_mid)
        except aiohttp.ClientError as error:
            # If Gatherer errors, omit the data for now
            # This can be appended on a case-by-case basis
            if error.code == 500:
                return  # Page doesn't work, nothing we can do
            else:
                print("Unknown error: ", error.code)
                return

        # Parse web page so we can gather all data from it
        soup_oracle = bs4.BeautifulSoup(html, 'html.parser')

        # Get Card Foreign Information
        c_foreign_info = await self.parse_foreign_info(soup_oracle)
        if c_foreign_info:
            card_info['foreignNames'] = c_foreign_info

    @staticmethod
    async def build_id_part(set_name: List[str],
                            card_mid: int,
                            card_info: dict
                            ) -> str:
        card_hash = hashlib.sha3_256()
        card_hash.update(set_name[0].encode('utf-8'))
        card_hash.update(str(card_mid).encode('utf-8'))
        card_hash.update(card_info['name'].encode('utf-8'))

        return card_hash.hexdigest()

    async def build_original_details(self,
                                     card_mid: int,
                                     card_info: dict,
                                     second_card: bool=False
                                     ) -> None:
        soup_print = await self.get_card_html(card_mid, True)

        # Determine if Card is Normal, Flip, or Split
        _, div_name, _ = await self.determine_layout_and_div_name(soup_print, second_card)

        # Get Card Original Type
        _, _, _, c_original_type = await self.parse_card_types(soup_print, div_name)
        if c_original_type:
            card_info['originalType'] = c_original_type

        # Get Card Original Text
        c_original_text, _ = await self.parse_card_text_and_color_identity(soup_print, div_name, None)
        if c_original_text:
            card_info['originalText'] = c_original_text

    async def build_card(self,
                         set_name: List[str],
                         card_mid: int,
                         other_cards_holder: Optional[List[object]],
                         second_card: bool=False
                         ) -> Dict[str, Any]:
        card_info: Dict[str, Any] = dict()

        await self.build_main_part(set_name, card_mid, card_info, other_cards_holder, second_card=second_card)
        await self.build_original_details(card_mid, card_info, second_card=second_card)
        await self.build_legalities_part(card_mid, card_info)
        await self.build_foreign_part(card_mid, card_info)

        card_info['cardHash'] = await self.build_id_part(set_name, card_mid, card_info)

        print('\tAdding {0} to {1}'.format(card_info['name'], set_name[0]))
        return card_info

    @staticmethod
    def add_card_layouts_inline(cards: list) -> None:
        for card_info in cards:
            if 'names' in card_info:
                sides = len(card_info['names'])
            else:
                sides = 1

            if sides == 1:
                if 'hand' in card_info:
                    card_layout = 'Vanguard'
                elif 'Scheme' in card_info['types']:
                    card_layout = 'Scheme'
                elif 'Plane' in card_info['types']:
                    card_layout = 'Plane'
                elif 'Phenomenon' in card_info['types']:
                    card_layout = 'Phenomenon'
                else:
                    card_layout = 'Normal'
            elif sides == 2:
                if 'transform' in card_info['text']:
                    card_layout = 'Double-Faced'
                elif 'aftermath' in card_info['text']:
                    card_layout = 'Aftermath'
                elif 'flip' in card_info['text']:
                    card_layout = 'Flip'
                elif 'split' in card_info['text']:
                    card_layout = 'Split'
                elif 'meld' in card_info['text']:
                    card_layout = 'Meld'
                else:
                    card_2_name = next(card2 for card2 in card_info['names'] if card_info['name'] != card2)
                    card_2_info = next(card2 for card2 in cards if card2['name'] == card_2_name)

                    if 'flip' in card_2_info['text']:
                        card_layout = 'Flip'
                    elif 'transform' in card_2_info['text']:
                        card_layout = 'Double-Faced'
                    else:
                        card_layout = 'Unknown'
            else:
                card_layout = 'Meld'

            card_info['layout'] = card_layout

    async def download_cards_by_mid_list(self, set_name: List[str], multiverse_ids: List[int]):
        additional_cards = []
        cards_in_set = []

        # start asyncio tasks for building each card
        futures = [
            self.loop.create_task(self.build_card(set_name, card_mid, additional_cards))
            for card_mid in multiverse_ids
        ]

        # then wait until all of them are completed
        with contextlib.suppress(ValueError):  # Happens if no cards are in the multiverse_ids
            await asyncio.wait(futures)
            for future in futures:
                card_future = future.result()
                cards_in_set.append(card_future)

        with contextlib.suppress(ValueError):  # If no double-sided cards, gracefully skip
            await asyncio.wait(additional_cards)
            print("Additional Cards found, lets work!")
            for future in additional_cards:
                card_future = future.result()
                cards_in_set.append(card_future)

        self.add_card_layouts_inline(cards_in_set)

        return cards_in_set

    async def build_set(self,
                        set_name: List[str],
                        language: str
                        ) -> List[int]:
        async def get_mids_for_downloading() -> List[int]:
            print('BuildSet: Building Set {}'.format(set_name[0]))

            urls_for_set = await get_checklist_urls(self.http_session, set_name)
            print('BuildSet: Acquired URLs for {}'.format(set_name[0]))

            # ids_to_return = [398434] # DEBUGGING IDs
            ids_to_return = await generate_mids_by_set(self.http_session, urls_for_set)
            return ids_to_return

        async def build_then_print_stuff(mids_for_set: List[int], lang: str = None) -> dict:
            if lang:
                set_stat = '{0}.{1}'.format(set_name[0], lang)
                set_output = '{0}.{1}'.format(set_name[1], lang)
            else:
                set_stat = str(set_name[0])
                set_output = str(set_name[1])

            print('BuildSet: Determined MIDs for {0}: {1}'.format(set_stat, mids_for_set))

            cards_holder = await self.download_cards_by_mid_list(set_name, mids_for_set)

            print('BuildSet: Applied Set Config options for {}'.format(set_stat))
            json_ready = await apply_set_config_options(set_name, cards_holder)

            print('BuildSet: Generated JSON for {}'.format(set_stat))
            with open_set_json(set_output, 'w') as fp:
                json.dump(json_ready, fp, indent=4, sort_keys=True, ensure_ascii=False)
                print('BuildSet: JSON written for {0} ({1})'.format(set_stat, set_name[1]))

            return json_ready

        async def build_foreign_language():
            if not is_set_file(set_name[1]):
                print('BuildSet: Set {0} not built in English. Do that first before {1}'.format(set_name[1], language))
                return None

            with open_set_json(set_name[1], 'r') as fp:
                json_input = json.load(fp)

            if ('translations' not in json_input.keys()) or (language not in json_input['translations'].keys()):
                print("BuildSet: Cannot translate {0} to {1}. Update set_configs".format(set_name[1], language))
                return None

            foreign_mids_for_set = []
            for card in json_input['cards']:
                full_name_lang_to_build = get_language_long_name(language)
                for lang_dict in card['foreignNames']:
                    if lang_dict['language'] == full_name_lang_to_build:
                        foreign_mids_for_set.append(int(lang_dict['multiverseid']))
                        break

            # Write to file the foreign build
            return await build_then_print_stuff(foreign_mids_for_set, language)

        if language == 'en':
            return await build_then_print_stuff(await get_mids_for_downloading())
        else:
            return await build_foreign_language()


async def apply_set_config_options(set_name: List[str],
                                   cards_dictionary: list
                                   ) -> Dict[Union[str, Any], Union[list, Any]]:
    return_product = dict()

    # Will search the tree of set_configs to find the file
    with open_set_config_json(set_name[1], 'r') as fp:
        file_response = json.load(fp)

    for key, value in file_response['SET'].items():
        return_product[key] = value

    if 'SET_CORRECTIONS' in file_response.keys():
        match_replace_rules = str(file_response['SET_CORRECTIONS'])
        match_replace_rules = ast.literal_eval(match_replace_rules)

        for replacement_rule in match_replace_rules:
            with contextlib.suppress(KeyError):  # If there's no match, it's deprecated
                replacement_match = replacement_rule['match']

            if 'replace' in replacement_rule.keys():
                fix_type = 'replace'
                replacement_update = replacement_rule['replace']
            elif 'fixForeignNames' in replacement_rule.keys():
                fix_type = 'fixForeignNames'
                replacement_update = replacement_rule['fixForeignNames']
            else:
                continue

            for key, value in replacement_match.items():
                if isinstance(value, list):
                    cards_to_modify = [
                        card
                        for card in cards_dictionary if key in card.keys() and card[key] in value
                    ]
                elif isinstance(value, str) or isinstance(value, int):
                    cards_to_modify = [
                        card
                        for card in cards_dictionary if key in card.keys() and card[key] == value
                    ]
                else:
                    continue

                if fix_type == 'replace':
                    for key_name, replacement in replacement_update.items():
                        for card in cards_to_modify:
                            card[key_name] = replacement
                elif fix_type == 'fixForeignNames':
                    for lang_replacements in replacement_update:
                        language_name = lang_replacements['language']
                        new_name = lang_replacements['name']

                        for card in cards_to_modify:
                            for foreign_names_field in card['foreignNames']:
                                if foreign_names_field['language'] == language_name:
                                    foreign_names_field['name'] = new_name

    return_product['cards'] = cards_dictionary

    return return_product




def determine_gatherer_sets(args: Dict[str, Union[bool, List[str]]]) -> List[List[str]]:
    def try_to_append(root_p, file_p):
        with pathlib.Path(root_p, file_p).open('r', encoding='utf8') as fp:
            this_set_name = json.load(fp)
            if 'SET' in this_set_name:
                all_sets.append([this_set_name['SET']['name'], file.split('.json')[0]])

    all_sets = list()
    if args['all_sets']:
        for root, _, files in os.walk(SET_CONFIG_DIR):
            for file in files:
                if file.endswith('.json'):
                    try_to_append(root, file)
    else:
        for root, _, files in os.walk(SET_CONFIG_DIR):
            for file in files:
                set_name = file.split('.json')[0]
                if set_name in args['sets']:
                    try_to_append(root, file)
                    args['sets'].remove(set_name)

        # Ensure all sets provided by the user are valid
        if len(args['sets']) > 0:
            print("MTGJSON: Invalid Set Code(s) provided: {}".format(args['sets']))
            exit(1)

    return all_sets
