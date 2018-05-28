import asyncio
import contextlib
import json
import os
import pathlib
from typing import Any, Dict, List, Optional, Union

import aiohttp
import bs4

from mtgjson4 import mtg_global, mtg_http, mtg_parse, mtg_storage, corrections
from mtgjson4.mtg_global import CardDescription

class MTGJSON:
    def __init__(self,
                 sets_to_build: List[List[str]],
                 session: Optional[aiohttp.ClientSession] = None,
                 loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
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

    async def get_card_html(self, card_mid: int, is_printed: bool = False) -> bs4.BeautifulSoup:
        """
        Gets the card details (first page) of a single card
        :param card_mid:
        :param is_printed:
        :return:
        """
        html = await mtg_http.get_card_details(self.http_session, card_mid, is_printed)
        soup = bs4.BeautifulSoup(html, 'html.parser')
        return soup

    @staticmethod
    def determine_layout_and_div_name(
            soup: bs4.BeautifulSoup,
            is_second_card: bool,
    ) -> List[Union[str, str, Optional[bool]]]:
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

        return [layout, div_name, add_additional_card]

    async def build_main_part(self,
                              set_name: List[str],
                              card_mid: int,
                              card_info: CardDescription,
                              other_cards_holder: Optional[List[object]],
                              second_card: bool = False) -> None:
        # Parse web page so we can gather all data from it
        soup_oracle = await self.get_card_html(card_mid)

        card_layout, div_name, add_other_card = self.determine_layout_and_div_name(soup_oracle, second_card)
        if add_other_card and other_cards_holder is not None:
            other_cards_holder.append(
                self.loop.create_task(self.build_card(set_name, card_mid, None, second_card=True)))

        card_info['multiverseid'] = int(card_mid)
        card_info['name'] = mtg_parse.parse_card_name(soup_oracle, div_name)
        card_info['cmc'] = mtg_parse.parse_card_cmc(soup_oracle, div_name)

        # Get other side's name for the user
        has_other, card_other_name = mtg_parse.parse_card_other_name(soup_oracle, div_name, card_layout)
        if has_other:
            card_info['names'] = [card_info['name'], card_other_name]

        # Get card's colors and mana cost
        card_colors, card_cost = mtg_parse.parse_colors_and_cost(soup_oracle, div_name)
        if card_colors:
            card_info['colors'] = card_colors
        if card_cost:
            card_info['manaCost'] = card_cost

        # Get Card Type(s)
        card_super_types, card_types, card_sub_types, full_type = mtg_parse.parse_card_types(soup_oracle, div_name)
        if card_super_types:
            card_info['supertypes'] = card_super_types
        if card_types:
            card_info['types'] = card_types
        if card_sub_types:
            card_info['subtypes'] = card_sub_types
        if full_type:
            card_info['type'] = full_type

        # Get Card Text and Color Identity
        card_info['text'], card_info['colorIdentity'] = mtg_parse.parse_card_text_and_color_identity(
            soup_oracle, div_name, card_colors)

        # Get Card Flavor Text
        c_flavor = mtg_parse.parse_card_flavor(soup_oracle, div_name)
        if c_flavor:
            card_info['flavor'] = c_flavor

        # Get Card P/T OR Loyalty OR Hand/Life
        c_power, c_toughness, c_loyalty, c_hand, c_life = mtg_parse.parse_card_pt_loyalty_vanguard(
            soup_oracle, div_name)
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
        card_info['rarity'] = mtg_parse.parse_card_rarity(soup_oracle, div_name)

        # Get Card Set Number
        c_number = mtg_parse.parse_card_number(soup_oracle, div_name)
        if c_number:
            card_info['number'] = c_number

        # Get Card Artist(s)
        card_info['artist'] = mtg_parse.parse_artists(soup_oracle, div_name)

        # Get Card Watermark
        c_watermark = mtg_parse.parse_watermark(soup_oracle, div_name)
        if c_watermark:
            card_info['watermark'] = c_watermark

        # Get Card Reserve List Status
        if card_info['name'] in mtg_global.RESERVE_LIST:
            card_info['reserved'] = True

        # Get Card Rulings
        c_rulings = mtg_parse.parse_rulings(soup_oracle, div_name)
        if c_rulings:
            card_info['rulings'] = c_rulings

        # Get Card Sets
        card_info['printings'] = mtg_parse.parse_card_sets(soup_oracle, div_name, set_name[1], self.sets_to_build)

        # Get Card Variations
        c_variations = mtg_parse.parse_card_variations(soup_oracle, div_name, card_mid)
        if c_variations:
            card_info['variations'] = c_variations

    async def build_legalities_part(self, card_mid: int, card_info: dict) -> None:
        try:
            html = await mtg_http.get_card_legalities(self.http_session, card_mid)
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
        c_legal = mtg_parse.parse_card_legal(soup_oracle)
        if c_legal:
            card_info['legalities'] = c_legal

    async def build_foreign_part(self, card_mid: int, card_info: dict) -> None:
        try:
            html = await mtg_http.get_card_foreign_details(self.http_session, card_mid)
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
        c_foreign_info = mtg_parse.parse_foreign_info(soup_oracle)
        if c_foreign_info:
            card_info['foreignNames'] = c_foreign_info

    async def build_original_details(self, card_mid: int, card_info: dict, second_card: bool = False) -> None:
        soup_print = await self.get_card_html(card_mid, True)

        # Determine if Card is Normal, Flip, or Split
        div_name = self.determine_layout_and_div_name(soup_print, second_card)[1]

        # Get Card Original Type
        c_original_type = mtg_parse.parse_card_types(soup_print, div_name)[3]
        if c_original_type:
            card_info['originalType'] = c_original_type

        # Get Card Original Text
        c_original_text = mtg_parse.parse_card_text_and_color_identity(soup_print, div_name, None)[0]
        if c_original_text:
            card_info['originalText'] = c_original_text

    async def build_card(self,
                         set_name: List[str],
                         card_mid: int,
                         other_cards_holder: Optional[List[object]],
                         second_card: bool = False) -> Dict[str, Any]:
        card_info: Dict[str, Any] = dict()

        await self.build_main_part(set_name, card_mid, card_info, other_cards_holder, second_card=second_card)
        await self.build_original_details(card_mid, card_info, second_card=second_card)
        await self.build_legalities_part(card_mid, card_info)
        await self.build_foreign_part(card_mid, card_info)

        card_info['cardHash'] = mtg_parse.build_id_part(set_name, card_mid, card_info)

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
            self.loop.create_task(self.build_card(set_name, card_mid, additional_cards)) for card_mid in multiverse_ids
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

    async def build_set(self, set_name: List[str], language: str) -> dict:
        async def get_mids_for_downloading() -> List[int]:
            print('BuildSet: Building Set {}'.format(set_name[0]))

            urls_for_set = await mtg_http.get_checklist_urls(self.http_session, set_name)
            print('BuildSet: Acquired {1} URLs for {0}'.format(set_name[0], len(urls_for_set)))

            # ids_to_return = [398434] # DEBUGGING IDs
            ids_to_return = await mtg_http.generate_mids_by_set(self.http_session, urls_for_set)
            return ids_to_return

        async def build_then_print_stuff(mids_for_set: List[int], lang: str = None) -> dict:
            if lang:
                set_stat = '{0}.{1}'.format(set_name[0], lang)
                set_output = '{0}.{1}'.format(set_name[1], lang)
            else:
                set_stat = str(set_name[0])
                set_output = str(set_name[1])

            print('BuildSet: Determined {1} MIDs for {0}'.format(set_stat, len(mids_for_set)))

            cards_holder = await self.download_cards_by_mid_list(set_name, mids_for_set)

            print('BuildSet: Applied Set Config options for {}'.format(set_stat))
            json_ready = await apply_set_config_options(set_name, cards_holder)

            print('BuildSet: Generated JSON for {}'.format(set_stat))
            with mtg_storage.open_set_json(set_output, 'w') as fp:
                json.dump(json_ready, fp, indent=4, sort_keys=True, ensure_ascii=False)
                print('BuildSet: JSON written for {0} ({1})'.format(set_stat, set_name[1]))

            return json_ready

        async def build_foreign_language() -> Optional[dict]:
            if not mtg_storage.is_set_file(set_name[1]):
                print('BuildSet: Set {0} not built in English. Do that first before {1}'.format(set_name[1], language))
                return None

            with mtg_storage.open_set_json(set_name[1], 'r') as fp:
                json_input = json.load(fp)

            if ('translations' not in json_input.keys()) or (language not in json_input['translations'].keys()):
                print("BuildSet: Cannot translate {0} to {1}. Update set_configs".format(set_name[1], language))
                return None

            foreign_mids_for_set = []
            for card in json_input['cards']:
                full_name_lang_to_build = mtg_global.get_language_long_name(language)
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
                                   cards_dictionary: List[CardDescription]) -> Dict[str, Union[list, Any]]:
    return_product = dict()

    # Will search the tree of set_configs to find the file
    with mtg_storage.open_set_config_json(set_name[1], 'r') as f:
        file_response = json.load(f)

    for key, value in file_response['SET'].items():
        return_product[key] = value

    if 'SET_CORRECTIONS' in file_response.keys():
        corrections.apply_corrections(file_response['SET_CORRECTIONS'], cards_dictionary)

    return_product['cards'] = cards_dictionary

    return return_product



def determine_gatherer_sets(args: Dict[str, Union[bool, List[str]]]) -> List[List[str]]:
    def try_to_append(root_p, file_p):
        with pathlib.Path(root_p, file_p).open('r', encoding='utf8') as f:
            this_set_name = json.load(f)
            if 'SET' in this_set_name:
                all_sets.append([this_set_name['SET']['name'], file.split('.json')[0]])

    all_sets = list()
    if args['all_sets']:
        for root, _, files in os.walk(mtg_storage.SET_CONFIG_DIR):
            for file in files:
                if file.endswith('.json'):
                    try_to_append(root, file)
    else:
        # Upper all the sets, and fix the Conflux set (Windows can't have CON files)
        set_args = ['CON_' if sa.upper() == 'CON' else sa for sa in args['sets']]

        for root, _, files in os.walk(mtg_storage.SET_CONFIG_DIR):
            for file in files:
                set_name: str = file.split('.json')[0]
                if str.upper(set_name) in set_args:
                    try_to_append(root, file)
                    set_args.remove(set_name)

        # Ensure all sets provided by the user are valid
        if set_args:
            print("MTGJSON: Invalid Set Code(s) provided: {}".format(args['sets']))
            exit(1)

    return all_sets
