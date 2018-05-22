import gathererSets

import urllib.parse
import urllib.request
from bs4 import BeautifulSoup


class GetChecklistURLs:
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
        """
        Get all the URLs necessary for sets in the gathererSets
        file and dictionary them.
        :return: Dictionary (Set, URLs)
        """
        sets_to_download = gathererSets.get_gatherer_sets()
        main_url = 'http://gatherer.wizards.com/Pages/Search/Default.aspx?{}'

        return_value = {}

        for card_set in sets_to_download:
            urls_to_download = []
            url_for_info = main_url.format(self.get_url_params(card_set, 0))

            with urllib.request.urlopen(url_for_info) as response:
                html = response.read()
                for i in range(0, self.get_page_count_for_set(html)):
                    urls_to_download.append(main_url.format(self.get_url_params(card_set, i)))

            # Insert new value
            return_value[card_set] = urls_to_download

        return return_value


class GenerateMIDsBySet:
    # Class Variable
    all_set_multiverse_ids = []
    set_name = ""

    def parse_urls(self, set_name, set_urls):
        self.set_name = set_name
        for url in set_urls:
            self.parse_url_for_m_ids(url)

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

    def __init__(self, set_name, multi_ids):
        self.set_name = set_name
        self.multiverse_ids = multi_ids
        self.create_cards()

    def create_cards(self):
        main_url = 'http://gatherer.wizards.com/Pages/Card/Details.aspx?{}'
        div_name = 'ctl00_ctl00_ctl00_MainContent_SubContent_SubContent_{}'

        for card_m_id in multiverse_ids:
            url_for_info = main_url.format(self.get_url_params(card_m_id))

            with urllib.request.urlopen(url_for_info) as response:
                html = response.read()
                card_info = {}

                """ Get Card Multiverse ID """
                card_info['multiverseid'] = card_m_id

                soup = BeautifulSoup(html.decode(), 'html.parser')

                """ Get Card Name """
                name_row = soup.find(id=div_name.format('nameRow'))
                name_row = name_row.findAll('div')[-1]
                card_name = str(name_row.contents)[6:].lstrip()[:-2]

                card_info['name'] = card_name
                print(card_name)

                """ Get Card CMC, Colors, and Color Identity (start) """
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
                print(card_cmc, card_color_identity, card_colors)

                """ Get Card Type(s) """
                card_super_types = []
                card_types = []
                card_sub_types = []
                type_row = soup.find(id=div_name.format('typeRow'))
                type_row = type_row.findAll('div')[-1]
                type_row = str(type_row.contents)[6:].lstrip()[:-2]

                if '—' in type_row:
                    type_split = type_row.split('—')

                    for value in type_split[0].split(' '):
                        if value in gathererSets.get_super_types():
                            card_super_types.append(value)
                        elif value in gathererSets.get_types():
                            card_types.append(value)

                    for value in type_split[1].split(' '):
                        card_sub_types.append(value)
                else:
                    for value in type_row.split(' '):
                        if value in gathererSets.get_super_types():
                            card_super_types.append(value)
                        elif value in gathererSets.get_types():
                            card_types.append(value)

                # Remove empty values from the lists
                card_super_types = list(filter(None, card_super_types))
                card_types = list(filter(None, card_types))
                card_sub_types = list(filter(None, card_sub_types))

                card_info['supertypes'] = card_super_types
                card_info['types'] = card_types
                card_info['subtypes'] = card_sub_types
                print(type_row, card_super_types, card_types, card_sub_types)

                text_row = soup.find(id=div_name.format('textRow'))
                text_row = text_row.findAll('div')[-1]
                text_row = str(text_row)[52:-6] # Cannot use .contents as it messes with images

                text_soup = BeautifulSoup(text_row, 'html.parser')
                for symbol in text_soup.findAll('img'):
                    symbol_value = symbol['alt']
                    symbol.replace_with("{{{0}}}".format(symbol_value))

                card_info['text'] = str(text_soup)
                print(text_soup)

                # Insert new value
                self.cards_in_set[card_m_id] = card_info
                print("--"*10)

    @staticmethod
    def get_url_params(card_m_id):
        url_params = urllib.parse.urlencode({
            'multiverseid': '{0}'.format(card_m_id),
            'printed': 'false'
        })
        return url_params

    def get_cards_in_set(self):
        return self.cards_in_set


if __name__ == '__main__':
    urls = GetChecklistURLs().get_key_with_urls()

    m_ids_holder = GenerateMIDsBySet()
    for key, value in urls.items():
        #m_ids_holder.parse_urls(key, value)
        #multiverse_ids = m_ids_holder.get_multiverse_ids_from_set()
        #m_ids_holder.clear()

        multiverse_ids = ['417574', '417575', '417576', '417577', '417578', '417579']

        cards_holder = DownloadsCardsByMIDList(key, multiverse_ids).get_cards_in_set()
        print(cards_holder)
