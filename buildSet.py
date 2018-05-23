# My Imports
import time

import sharedInfo

# Library Imports
import bs4
import json
import multiprocessing
import re
import urllib.parse
import urllib.request


class GetChecklistURLs:
    set_to_download = ''

    def start(self, magic_set_name):
        self.set_to_download = magic_set_name
        return self.get_key_with_urls()

    @staticmethod
    def get_page_count_for_set(html_data):
        try:
            # Get the last instance of 'pagingcontrols' and get the page
            # number from the URL it contains
            soup = bs4.BeautifulSoup(html_data.decode(), 'html.parser')
            soup = soup.select('div[class^=pagingcontrols]')[-1]
            soup = soup.findAll('a')

            # If it sees '1,2,3>' will take the '3' instead of '>'
            if '&gt;' in str(soup[-1]):
                soup = soup[-2]
            else:
                soup = soup[-1]

            total_pages = str(soup).split('page=')[1].split('&')[0]
        except IndexError:
            total_pages = 0

        return int(total_pages)+1

    @staticmethod
    def get_url_params(card_set, page_number=0):
        url_params = urllib.parse.urlencode({
            'output': 'checklist',
            'sort': 'cn+',
            'action': 'advanced',
            'special': 'true',
            'set': '["{0}"]'.format(card_set),
            'page': page_number
        })

        return url_params

    def get_key_with_urls(self):
        main_url = 'http://gatherer.wizards.com/Pages/Search/Default.aspx?{}'

        urls_to_download = []
        url_for_info = main_url.format(self.get_url_params(self.set_to_download, 0))

        with urllib.request.urlopen(url_for_info) as response:
            html = response.read()
            for i in range(0, self.get_page_count_for_set(html)):
                urls_to_download.append(main_url.format(self.get_url_params(self.set_to_download, i)))

        return urls_to_download


class GenerateMIDsBySet:
    # Class Variable
    all_set_multiverse_ids = []
    set_name = ''

    def start(self, set_name, set_urls):
        self.set_name = set_name
        for url in set_urls:
            self.parse_url_for_m_ids(url)

        return self.get_multiverse_ids_from_set()

    def parse_url_for_m_ids(self, url):
        with urllib.request.urlopen(url) as response:
            html = response.read()
            soup = bs4.BeautifulSoup(html.decode(), 'html.parser')

            # All cards on the page
            soup = soup.findAll('a', {'class': 'nameLink'})
            for card_info in soup:
                card_m_id = str(card_info).split('multiverseid=')[1].split('"')[0]
                self.all_set_multiverse_ids.append(card_m_id)

    def get_multiverse_ids_from_set(self):
        return self.all_set_multiverse_ids

    def clear(self):
        self.set_name = ''
        self.all_set_multiverse_ids = []


class DownloadsCardsByMIDList:
    # Class Variables
    set_name = ''
    multiverse_ids = []
    cards_in_set = {}
    main_url = 'http://gatherer.wizards.com/Pages/Card/Details.aspx?{}'
    magic_colors = ['W', 'U', 'B', 'R', 'G']

    def start(self, set_name, multi_ids):
        self.set_name = set_name
        self.multiverse_ids = multi_ids

        self.create_cards()

        self.add_layouts()
        return self.get_cards_in_set()

    def create_cards(self):
        # TODO: Parallelize this somehow
        for card_m_id in self.multiverse_ids:
            self.build_card(card_m_id)

    def build_card(self, card_m_id):
        url_for_info = self.main_url.format(self.get_url_params(card_m_id))

        with urllib.request.urlopen(url_for_info) as response:
            html = response.read()
            card_info = {}

            # Parse webpage so we can gather all data from it
            soup = bs4.BeautifulSoup(html.decode(), 'html.parser')

            """ Get Card Multiverse ID """
            card_info['multiverseid'] = int(card_m_id)

            """ Determine if Card is Normal, Flip, or Split """
            div_name = 'ctl00_ctl00_ctl00_MainContent_SubContent_SubContent_{}'
            card_layout = ''
            cards_total = len(soup.select('table[class^=cardDetails]'))
            if cards_total == 1:
                card_layout = 'normal'
            elif cards_total == 2:
                card_layout = 'double'
                div_name = div_name[:-3] + '_ctl02_{}'

            """ Get Card Name """
            try:
                name_row = soup.find(id=div_name.format('nameRow'))
                name_row = name_row.findAll('div')[-1]
                card_name = name_row.get_text(strip=True)
                card_info['name'] = card_name

                # Get other side's name for the user
                if card_layout == 'double':
                    other_div_name = div_name.replace('02', '03')
                    other_name_row = soup.find(id=other_div_name.format('nameRow'))
                    other_name_row = other_name_row.findAll('div')[-1]
                    card_other_name = other_name_row.get_text(strip=True)
                    card_info['names'] = [card_name, card_other_name]
            except AttributeError:
                pass

            """ Get Card CMC """
            try:
                cmc_row = soup.find(id=div_name.format('cmcRow'))
                cmc_row = cmc_row.findAll('div')[-1]
                card_cmc = cmc_row.get_text(strip=True)
                card_info['cmc'] = int(card_cmc)
            except AttributeError:
                card_info['cmc'] = 0
                pass

            """ Get Card Colors, Cost, and Color Identity (start) """
            try:
                mana_row = soup.find(id=div_name.format('manaRow'))
                mana_row = mana_row.findAll('div')[-1]
                mana_row = mana_row.findAll('img')

                card_colors = []
                card_cost = ''
                card_color_identity = []

                for symbol in mana_row:
                    symbol_value = symbol['alt']
                    symbol_mapped = sharedInfo.get_symbol_short_name(symbol_value)
                    card_cost += '{{{0}}}'.format(symbol_mapped)
                    if not symbol_value.isdigit() and symbol_mapped in self.magic_colors:
                        card_color_identity.append(symbol_mapped)
                        card_colors.append(symbol_mapped)

                # Remove duplicates
                card_colors = list(set(card_colors))

                card_info['colors'] = card_colors
                card_info['manaCost'] = card_cost
                card_info['colorIdentity'] = card_color_identity
            except AttributeError:
                card_info['colors'] = []
                card_info['manaCost'] = ''
                card_info['colorIdentity'] = []
                pass

            """ Get Card Type(s) """
            try:
                card_super_types = []
                card_layouts = []
                card_sub_types = []
                type_row = soup.find(id=div_name.format('typeRow'))
                type_row = type_row.findAll('div')[-1]
                type_row = type_row.get_text(strip=True)

                card_full_type = type_row.replace('  ', ' ')

                if '—' in type_row:
                    type_split = type_row.split('—')

                    for value in type_split[0].split(' '):
                        if value in sharedInfo.get_super_types():
                            card_super_types.append(value)
                        elif value in sharedInfo.get_types():
                            card_layouts.append(value)

                    for value in type_split[1].split(' '):
                        card_sub_types.append(value)
                else:
                    for value in type_row.split(' '):
                        if value in sharedInfo.get_super_types():
                            card_super_types.append(value)
                        elif value in sharedInfo.get_types():
                            card_layouts.append(value)

                # Remove empty values from the lists
                card_super_types = list(filter(None, card_super_types))
                card_layouts = list(filter(None, card_layouts))
                card_sub_types = list(filter(None, card_sub_types))

                card_info['supertypes'] = card_super_types
                card_info['types'] = card_layouts
                card_info['subtypes'] = card_sub_types
                card_info['type'] = card_full_type
            except AttributeError:
                pass

            """ Get Card Text and Color Identity (remaining) """
            try:
                text_row = soup.find(id=div_name.format('textRow'))
                text_row = text_row.select('div[class^=cardtextbox]')

                card_text = ''
                for div in text_row:
                    # Start by replacing all images with alternative text
                    images = div.findAll('img')
                    for symbol in images:
                        symbol_value = symbol['alt']
                        symbol_mapped = sharedInfo.get_symbol_short_name(symbol_value)
                        symbol.replace_with('{{{0}}}'.format(symbol_mapped))
                        if not symbol_mapped.isdigit() and symbol_mapped in self.magic_colors:
                            card_info['colorIdentity'] += symbol_mapped

                    # Next, just add the card text, line by line
                    card_text += div.get_text() + '\n'

                card_info['text'] = card_text[:-1]  # Remove last '\n'
                card_info['colorIdentity'] = list(set(card_info['colorIdentity']))
            except AttributeError:
                pass

            """ Get Card Flavor Text """
            try:
                flavor_row = soup.find(id=div_name.format('flavorRow'))
                flavor_row = flavor_row.select('div[class^=flavortextbox]')

                card_flavor_text = ''
                for div in flavor_row:
                    card_flavor_text += div.get_text() + '\n'

                card_info['flavor'] = card_flavor_text[:-1]  # Remove last '\n'
            except AttributeError:
                pass

            """ Get Card P/T OR Loyalty OR Hand/Life """
            try:
                pt_row = soup.find(id=div_name.format('ptRow'))
                pt_row = pt_row.findAll('div')[-1]
                pt_row = pt_row.get_text(strip=True)

                # If Vanguard
                if 'Hand Modifier' in pt_row:
                    pt_row = pt_row.split('\xa0,\xa0')
                    card_hand_mod = pt_row[0].split(' ')[-1]
                    card_life_mod = pt_row[1].split(' ')[-1][:-1]

                    card_info['hand'] = card_hand_mod
                    card_info['life'] = card_life_mod
                    pass

                pt_row = pt_row.split('/')
                if len(pt_row) == 2:
                    card_power = pt_row[0].strip()
                    card_toughness = pt_row[1].strip()
                    card_info['power'] = card_power
                    card_info['toughness'] = card_toughness
                else:
                    card_loyalty = pt_row[0].strip()
                    card_info['loyalty'] = card_loyalty
            except (AttributeError, IndexError):
                pass

            """ Get Card Rarity """
            try:
                rarity_row = soup.find(id=div_name.format('rarityRow'))
                rarity_row = rarity_row.findAll('div')[-1]
                card_rarity = rarity_row.find('span').get_text(strip=True)
                card_info['rarity'] = card_rarity
            except AttributeError:
                pass

            """ Get Card Set Number """
            try:
                number_row = soup.find(id=div_name.format('numberRow'))
                number_row = number_row.findAll('div')[-1]
                card_number = number_row.get_text(strip=True)
                card_info['number'] = card_number
            except AttributeError:
                pass

            """ Get Card Artist """
            try:
                artist_row = soup.find(id=div_name.format('artistRow'))
                artist_row = artist_row.findAll('div')[-1]
                card_artist = artist_row.find('a').get_text(strip=True)
                card_info['artist'] = card_artist
            except AttributeError:
                pass

            """ Get Card Watermark """
            try:
                watermark_row = soup.find(id=div_name.format('markRow'))
                watermark_row = watermark_row.findAll('div')[-1]
                card_watermark = watermark_row.get_text(strip=True)
                card_info['watermark'] = card_watermark
            except AttributeError:
                pass

            """ Get Card Rulings """
            try:
                rulings_row = soup.find(id=div_name.format('rulingsRow'))
                rulings_dates = rulings_row.findAll('td', id=re.compile(r'\w*_rulingDate\b'))
                rulings_text = rulings_row.findAll('td', id=re.compile(r'\w*_rulingText\b'))

                card_rulings = []
                for i in range(0, len(rulings_dates)):
                    card_rulings.append({
                        'date': rulings_dates[i].get_text(),
                        'text': rulings_text[i].get_text()
                    })

                card_info['rulings'] = card_rulings
            except AttributeError:
                pass

            """ Get Card Sets """
            try:
                sets_row = soup.find(id=div_name.format('otherSetsRow'))
                images = sets_row.findAll('img')

                card_sets = []
                for symbol in images:
                    symbol_value = symbol['alt'].split('(')[0].strip()
                    card_sets.append(symbol_value)

                card_info['printings'] = card_sets

            except AttributeError:
                pass

            # TODO: Missing types
            # id, layout, variations, border, timeshifted, reserved,
            # starter, mciNumber, scryfallNumber
            # EX: foreignNames, legalities

            # Insert new value
            self.cards_in_set[card_m_id] = card_info
            print('Added {0} to {1}'.format(card_info['name'], self.set_name))

    def add_layouts(self):
        """
        try:
            if cards_total == 1:
                if card_info.get('hand'):
                    card_layout = 'Vanguard'
                elif 'Scheme' in card_info.get('types'):
                    card_layout = 'Scheme'
                elif 'Plane' in card_info.get('types'):
                    card_layout = 'Plane'
                elif 'Phenomenon' in card_info.get('types'):
                    card_layout = 'Phenomenon'
                else:
                    card_layout = 'Normal'
            elif cards_total == 2:
                if "transform" in card_info.get('text'):
                    card_layout = 'Double-Faced'

                # split, flip, double-faced, aftermath, meld

            card_info['layout'] = card_layout
        except AttributeError:
            pass
        """
        pass

    @staticmethod
    def get_url_params(card_m_id):
        url_params = urllib.parse.urlencode({
            'multiverseid': '{}'.format(card_m_id),
            'printed': 'false'
        })
        return url_params

    def get_cards_in_set(self):
        return self.cards_in_set


def build_set(set_name):
        print('S2F: {}'.format(set_name))

        urls_for_set = GetChecklistURLs().start(set_name)
        print('S2F: {}'.format(urls_for_set))

        m_ids_for_set = GenerateMIDsBySet().start(set_name, urls_for_set)
        # m_ids_for_set = [442051, 435172, 182290, 435173, 435176, 366360, 370424, 6528, 212578, 423590, 423582]
        print('S2F: {0} with {1} ids'.format(m_ids_for_set, len(m_ids_for_set)))

        cards_holder = DownloadsCardsByMIDList().start(set_name, m_ids_for_set)
        print('S2F: {}'.format(cards_holder))

        with open('outputs/{}.json'.format(set_name), 'w') as fp:
            fp.write(json.dumps(cards_holder, sort_keys=True, indent=4))
            print('S2F: {} written'.format(fp.name))


if __name__ == '__main__':
    start_time = time.time()

    pool = multiprocessing.Pool()
    results = []

    for magic_set in sharedInfo.get_gatherer_sets():
        results.append(pool.apply_async(build_set, args=(magic_set,)))

    pool.close()
    pool.join()

    end_time = time.time()
    print("Time: {}".format(end_time - start_time))
