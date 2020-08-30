import csv
import datetime
import os
import re
from pathlib import Path
from typing import List, Tuple, Any, Dict

from src.request import get_json

# folder with csv files
DATA_FOLDER = Path('data')
os.makedirs(DATA_FOLDER, exist_ok=True)

# delimiter in csv files
DELIMITER = '\t'

# for metadata writing
FIELDNAMES = (
    'path', 'header', 'created', 'author', 'birthday', 'header_trans',
    'author_trans', 'translator', 'date_trans', 'sphere', 'lang', 'lang_trans'
)

BASE_URL = "https://api.vk.com/method"
SEARCH_ON_WALL = "wall.search"


class VKCrawler:
    def __init__(self,
                 access_token: str,
                 method: str = SEARCH_ON_WALL,
                 **params) -> None:
        """ """
        self._url = BASE_URL
        self._method = method

        self._access_token = access_token
        self._params = {
            **params,
            'v': '5.122',
            'access_token': self.access_token
        }
        self._posts = []
        self._parsed_posts = []

    @property
    def url(self) -> str:
        """
        :return: str, url of vk dev joined with method.
        """
        return f"{self._url}/{self._method}"

    @property
    def posts(self) -> List[Dict[str, Any]]:
        """
        :return: list of dicts, posts from VK.
        """
        return self._posts

    @property
    def parsed_posts(self) -> List[Dict[str, Any]]:
        """
        :return: list of dict, parsed posts.
        """
        return self._parsed_posts

    @property
    def access_token(self) -> str:
        """
        :return: str, access token.
        """
        return self._access_token

    def request(self,
                count: int) -> None:
        """ Request posts from vk and parse them.
        Update posts list and list of parsed posts.

        :param count: int, count of posts.
        :return: None.
        """
        posts = get_json(self.url, **self._params, count=count)
        posts = [
            item['response']['items']
            for item in posts
        ]
        # convert matrix to list
        self._posts = sum(posts, [])
        self._parsed_posts = self._parse_posts()

    @staticmethod
    def _swap_langs(pairs: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
        """ Swap languages if the first one is not Russian.

        :param pairs: list of tuples, pairs: Russian – Chinese (expected).
        :return: list of tuples, pairs with corrected order.
        """
        if re.search(r'[а-яёА-ЯЁ]', pairs[0][0]):
            return pairs
        return [
            (ru, ch)
            for ch, ru in pairs
        ]

    @staticmethod
    def _get_text(text: str) -> Dict[str, Any]:
        """ Parse text to its headers and list of tuples:
        Russian text, Chinese text.

        Dict format {
            'header': header in Chinese.
            'header_trans': header in Russian,
            'text': [(Russian, Chinese), (Russian, Chinese)...]
        }

        :param text: str, text to parse.
        :return: dict of str with the format.
        """
        patter = re.compile(
            r'#Новости_на_двух_языках', flags=re.IGNORECASE)
        assert patter.search(text), "There is no the hashtag"

        # here is '#Новости_на_двух_языках', remove it
        paragraphs = text.split('\n')[2:]

        paragraphs = [
            text.strip()
            for text in paragraphs
            if len(text) > 1
        ]
        pairs = list(zip(paragraphs[::2], paragraphs[::2]))
        # swap languages if it is demanded
        pairs = VKCrawler._swap_langs(pairs)

        header_trans, header = pairs[0]
        return {
            'header': header,
            'header_trans': header_trans,
            'text': pairs[1:]
        }

    @staticmethod
    def _get_date(timestamp: int) -> datetime.date:
        """ Convert date from timestamp to datetime.

        :param timestamp: int, date in timestamp.
        :return: date.
        """
        dt = datetime.datetime.fromtimestamp(timestamp)
        return dt.date()

    @staticmethod
    def _parse_post(post: Dict[str, Any]) -> Dict[str, Any]:
        """ Get all info from the post.

        Dict format {
            'title': (Russian, Chinese),
            'text': [(Russian, Chinese), (Russian, Chinese)...],
            'date': date, str format m/d/y
        }

        :param post: dict of str, post to parse.
        :return: dict of str with the format
        """
        date = VKCrawler._get_date(post['date'])
        date = date.strftime("%m/%d/%Y")
        try:
            title_and_text = VKCrawler._get_text(post['text'])
        except AssertionError as e:
            print(e, post, sep='\n')
            return {}

        return {
            **title_and_text,
            'date': date,
        }

    def _parse_posts(self) -> List[Dict[str, Any]]:
        """ Parse all posts to dict format: {
           'header': header in Chinese
           'header_trans': header in Russian,
            'text': [(Russian, Chinese), (Russian, Chinese)...],
            'date': date, str format m/d/y
        }

        :return: list of parsed posts.
        """
        parsed_posts = [
            VKCrawler._parse_post(post)
            for post in self.posts
        ]
        return parsed_posts

    @staticmethod
    def _dump_one(post: Dict[str, Any],
                  filepath: Path) -> None:
        """ Dump one post to the csv file.

        :param post: dict of str, post to dump.
        :param filepath: Path to the file.
        :return: None.
        """
        with filepath.open('w', newline='', encoding='utf-8') as f:
            writer = csv.writer(
                f, delimiter=DELIMITER, quoting=csv.QUOTE_MINIMAL)
            for pair_ch_ru in post['text']:
                writer.writerow(pair_ch_ru)

    @staticmethod
    def _create_filename(title: str) -> Path:
        """ Remove wrong symbols from the title,
        replace spaces to the '_', add DATA_OLDER
        as a parent and short the filename to 16 symbols.

        :param title: str, title of the post.
        :return: Path to the csv file in the DATA_FOLDER.
        """
        title = [
            symbol if symbol != ' ' else '_'
            for symbol in title
            if symbol.isalpha() or symbol == ' '
        ]
        filename = ''.join(title)[:16]
        return DATA_FOLDER / f"{filename}.csv"

    def dump_all(self) -> None:
        """ Dump to csv all posts and write to
        other file metadata.

        Filename is the first 16 symbols of
        Russian translation of the title.

        :return: None.
        """
        metadata = []
        for post in self.parsed_posts:
            header = post.pop('header')
            header_trans = post.pop('header_trans')
            date = post.pop('date')

            # name is the first 16 symbols of Russian
            # translation of the title
            path = VKCrawler._create_filename(header_trans)

            md = {
                'path': path.name,
                'header': header,
                'header_trans': header_trans,
                'created': date
            }
            metadata += [md]

            VKCrawler._dump_one(post, path)
        VKCrawler._dump_metadata(metadata)

    @staticmethod
    def _dump_metadata(metadata: List[Dict[str, Any]]) -> None:
        """ Dump metadata to the csv file.

        :param metadata: list of tuples, metadata to dump.
        :return: None.
        """
        path = DATA_FOLDER / 'metadata.csv'
        with path.open('w', newline='', encoding='utf-8') as f:

            writer = csv.DictWriter(
                f, fieldnames=FIELDNAMES,
                delimiter=DELIMITER,
                quoting=csv.QUOTE_MINIMAL
            )

            writer.writerows(metadata)
