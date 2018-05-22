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
        main_url = "http://gatherer.wizards.com/Pages/Search/Default.aspx?{}"

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


class DownloadCardsBySet:
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
        return {self.set_name: self.all_set_multiverse_ids}

    def clear(self):
        self.set_name = ""
        self.all_set_multiverse_ids = []


if __name__ == "__main__":
    urls = GetChecklistURLs().get_key_with_urls()

    download_holder = DownloadCardsBySet()
    for key, value in urls.items():
        download_holder.parse_urls(key, value)

        multiverse_ids = download_holder.get_multiverse_ids_from_set()
        download_holder.clear()
        print(multiverse_ids)
