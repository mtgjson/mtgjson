import sharedInfo

import json
import urllib.parse
import urllib.request
from bs4 import BeautifulSoup
from multiprocessing import Pool


class GetChecklistURLs:
    set_to_download = ""

    def start(self, magic_set):
        self.set_to_download = magic_set
        return self.get_key_with_urls()

    @staticmethod
    def get_page_count_for_set(html_data):
        """
        Function will check the data downloaded from the initial
        page for how many pages exist in the checklist.
        :param html_data: Binary data
        :return: How many pages exist (pages are 0 indexed)
        """
        try:
            # Get the last instance of pagingcontrols and get the page
            # number from the URL it contains
            soup = BeautifulSoup(html_data.decode(), 'html.parser')
            soup = soup.select('div[class^=pagingcontrols]')[-1]
            soup = soup.select('a')[-1]
            total_pages = str(soup).split('page=')[1].split('&')[0]
        except IndexError:
            total_pages = 0

        return int(total_pages)+1

    @staticmethod
    def get_url_params(card_set, page_number=0):
        """
        Gets what the URL should contain to
        properly get the cards in the set.
        :param card_set: Set name
        :param page_number: What page to get (Default 0)
        :return: Encoded parameters
        """
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
    set_name = ""

    def start(self, set_name, set_urls):
        self.set_name = set_name
        for url in set_urls:
            self.parse_url_for_m_ids(url)

        return self.get_multiverse_ids_from_set()

    def parse_url_for_m_ids(self, url):
        with urllib.request.urlopen(url) as response:
            html = response.read()
            soup = BeautifulSoup(html.decode(), 'html.parser')

            # All cards on the page
            soup = soup.findAll('a', {'class': 'nameLink'})
            for card_info in soup:
                card_m_id = str(card_info).split('multiverseid=')[1].split('"')[0]
                self.all_set_multiverse_ids.append(card_m_id)

    def get_multiverse_ids_from_set(self):
        return self.all_set_multiverse_ids

    def clear(self):
        self.set_name = ""
        self.all_set_multiverse_ids = []


class DownloadsCardsByMIDList:
    # Class Variables
    set_name = ""
    multiverse_ids = []
    cards_in_set = {}

    def start(self, set_name, multi_ids):
        self.set_name = set_name
        self.multiverse_ids = multi_ids
        self.create_cards()
        return self.get_cards_in_set()

    def create_cards(self):
        main_url = 'http://gatherer.wizards.com/Pages/Card/Details.aspx?{}'
        div_name = 'ctl00_ctl00_ctl00_MainContent_SubContent_SubContent_{}'

        for card_m_id in self.multiverse_ids:
            url_for_info = main_url.format(self.get_url_params(card_m_id))

            with urllib.request.urlopen(url_for_info) as response:
                html = response.read()
                card_info = {}

                # Parse webpage so we can gather all data from it
                soup = BeautifulSoup(html.decode(), 'html.parser')

                """ Get Card Multiverse ID """
                card_info['multiverseid'] = card_m_id

                """ Get Card Name """
                try:
                    name_row = soup.find(id=div_name.format('nameRow'))
                    name_row = name_row.findAll('div')[-1]
                    card_name = str(name_row.contents)[6:].lstrip()[:-2]
                    card_info['name'] = card_name
                except AttributeError:
                    pass

                """ Get Card CMC, Colors, and Color Identity (start) """
                try:
                    mana_row = soup.find(id=div_name.format('manaRow'))
                    mana_row = mana_row.findAll('div')[-1]
                    mana_row = mana_row.findAll('img')

                    card_cmc = 0
                    card_color_identity = []
                    card_colors = []

                    for symbol in mana_row:
                        symbol_value = symbol['alt']
                        if symbol_value.isdigit():
                            card_cmc += int(symbol_value)
                        elif symbol_value != 'X':
                            card_cmc += 1
                            card_color_identity.append(symbol_value)
                            card_colors.append(symbol_value[0])

                    # Remove duplicates
                    card_colors = list(set(card_colors))

                    card_info['cmc'] = card_cmc
                    card_info['colors'] = card_colors
                except AttributeError:
                    card_info['cmc'] = 0
                    pass

                """ Get Card Type(s) """
                try:
                    card_super_types = []
                    card_types = []
                    card_sub_types = []
                    type_row = soup.find(id=div_name.format('typeRow'))
                    type_row = type_row.findAll('div')[-1]
                    type_row = str(type_row.contents)[6:].lstrip()[:-2]

                    if '—' in type_row:
                        type_split = type_row.split('—')

                        for value in type_split[0].split(' '):
                            if value in sharedInfo.get_super_types():
                                card_super_types.append(value)
                            elif value in sharedInfo.get_types():
                                card_types.append(value)

                        for value in type_split[1].split(' '):
                            card_sub_types.append(value)
                    else:
                        for value in type_row.split(' '):
                            if value in sharedInfo.get_super_types():
                                card_super_types.append(value)
                            elif value in sharedInfo.get_types():
                                card_types.append(value)

                    # Remove empty values from the lists
                    card_super_types = list(filter(None, card_super_types))
                    card_types = list(filter(None, card_types))
                    card_sub_types = list(filter(None, card_sub_types))

                    card_info['supertypes'] = card_super_types
                    card_info['types'] = card_types
                    card_info['subtypes'] = card_sub_types
                except AttributeError:
                    pass

                """ Get Card Text """
                try:
                    text_row = soup.find(id=div_name.format('textRow'))
                    text_row = text_row.findAll('div')[-1]
                    text_row = str(text_row)[52:-6] # Cannot use .contents as it messes with images

                    text_soup = BeautifulSoup(text_row, 'html.parser')
                    for symbol in text_soup.findAll('img'):
                        symbol_value = symbol['alt']
                        symbol.replace_with("{{{0}}}".format(symbol_value))

                    card_info['text'] = str(text_soup)
                except AttributeError:
                    pass

                """ Get Card P/T (If Applicable) """
                try:
                    pt_row = soup.find(id=div_name.format('ptRow'))
                    pt_row = pt_row.findAll('div')[-1]
                    pt_row = str(pt_row.contents)[6:].lstrip()[:-2]
                    pt_row = pt_row.split('/')

                    card_power = pt_row[0].strip()
                    card_toughness = pt_row[1].strip()
                    card_info['power'] = card_power
                    card_info['toughness'] = card_toughness
                except (AttributeError, IndexError):
                    pass

                """ Get Card Rarity """
                try:
                    rarity_row = soup.find(id=div_name.format('rarityRow'))
                    rarity_row = rarity_row.findAll('div')[-1]
                    rarity_row = rarity_row.find('span').contents
                    card_rarity = str(rarity_row)[2:].lstrip()[:-2]
                    card_info['rarity'] = card_rarity
                except AttributeError:
                    pass

                """ Get Card Set Number """
                try:
                    number_row = soup.find(id=div_name.format('numberRow'))
                    number_row = number_row.findAll('div')[-1]
                    card_number = str(number_row.contents)[6:].lstrip()[:-2]
                    card_info['number'] = card_number
                except AttributeError:
                    pass

                """ Get Card Artist """
                try:
                    artist_row = soup.find(id=div_name.format('artistRow'))
                    artist_row = artist_row.findAll('div')[-1]
                    artist_row = artist_row.find('a').contents
                    card_artist = str(artist_row)[2:].lstrip()[:-2]
                    card_info['artist'] = card_artist
                except AttributeError:
                    pass

                # Insert new value
                self.cards_in_set[card_m_id] = card_info

    @staticmethod
    def get_url_params(card_m_id):
        url_params = urllib.parse.urlencode({
            'multiverseid': '{}'.format(card_m_id),
            'printed': 'false'
        })
        return url_params

    def get_cards_in_set(self):
        return self.cards_in_set


class StartToFinishForSet:
    def __init__(self, set_name):
        print("S2F:{}".format(set_name))

        urls_for_set = GetChecklistURLs().start(set_name)
        print("S2F:{}".format(urls_for_set))

        m_ids_for_set = GenerateMIDsBySet().start(set_name, urls_for_set)
        print("S2F:{}".format(m_ids_for_set))

        cards_holder = DownloadsCardsByMIDList().start(set_name, m_ids_for_set)
        print("S2F:{}".format(cards_holder))

        with open("outputs/{}.json".format(set_name), "w") as fp:
            fp.write(json.dumps(cards_holder, sort_keys=True, indent=4))
            print("S2F:{} written".format(fp.name))
            fp.close()


if __name__ == '__main__':
    pool = Pool()
    results = []

    for magic_set in sharedInfo.get_gatherer_sets():
        results.append(pool.apply_async(StartToFinishForSet, args=(magic_set,)))

    pool.close()
    pool.join()

    results = [r.get() for r in results]
