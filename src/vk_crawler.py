import csv
import datetime
import os
import re
from pathlib import Path
from typing import List, Tuple, Any, Dict

from src.request import get_json

# Dict[str, Any]
Sdict = Dict[str, Any]
# pairs with languages
TL = Tuple[str, str]

# folder with csv files
DATA_FOLDER = Path('data') / 'VK'
# folder with files with skipped posts
SKIPPED_POSTS_FOLDER = DATA_FOLDER / 'skipped_posts'
os.makedirs(SKIPPED_POSTS_FOLDER, exist_ok=True)

# delimiter in csv files
DELIMITER = '\t'
ENCODING = 'utf-16'

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
        """ Init the crawler.

        :param access_token: str, access token.
        :param method: str, method of VK API you want to use.
        :param params: some keywords to request.
        See documentation how to set it.
        :return: None.
        """
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
        self._results_count = self._get_results_count()

        self._skipped_posts = []

        self._dateformat = "%m/%d/%Y"
        self._data_folder = DATA_FOLDER
        self._skipped_posts_folder = SKIPPED_POSTS_FOLDER

    @property
    def skipped_posts_folder(self) -> Path:
        """
        :return: Path to the folder with
        files with skipped posts.
        """
        return self._skipped_posts_folder

    @property
    def data_folder(self) -> Path:
        """
        :return: Path to data folder with all files.
        """
        return self._data_folder

    @property
    def skipped_posts(self) -> List[Dict]:
        """
        :return: list of dicts, posts have not been parsed.
        """
        return self._skipped_posts

    @property
    def results_count(self) -> int:
        """
        :return: int, count of results of the request.
        """
        return self._results_count

    @property
    def url(self) -> str:
        """
        :return: str, url of vk dev joined with the method.
        """
        return f"{self._url}/{self._method}"

    @property
    def posts(self) -> List[Sdict]:
        """
        :return: list of dicts, posts from VK.
        """
        return self._posts

    @property
    def parsed_posts(self) -> List[Sdict]:
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

    def update(self,
               old_results_count: int) -> None:
        """ Get all new posts.

        :param old_results_count: int, old count of results (=count of docs).
        From here count of new posts will be calculated.
        :return: None.
        """
        if self.results_count < old_results_count:
            raise ValueError(
                "Old results count must be <= than current count")

        new_count = self.results_count - old_results_count
        self.request(new_count)

    def request(self,
                count: int) -> None:
        """ Request posts from vk and parse them.
        Update posts list and list of parsed posts.

        :param count: int, count of posts.
        :return: None.
        """
        if count > self.results_count:
            raise ValueError(f"There are less results: '{self.results_count}'")

        posts = get_json(self.url, **self._params, count=count)
        posts = [
            item['response']['items']
            for item in posts
        ]
        # convert matrix to list
        self._posts = sum(posts, [])
        self._parsed_posts = self._parse_posts()

    def _get_results_count(self) -> int:
        """ Get count of results found in VK.

        :return: int, count of results.
        """
        response = get_json(self.url, **self._params, count=1)
        return response[0]['response']['count']

    @staticmethod
    def _swap_langs(pairs: List[TL]) -> List[TL]:
        """ Swap languages if the first one is not Russian.

        :param pairs: list of tuples, pairs: Russian – Chinese (expected).
        :return: list of tuples, pairs with corrected order.
        """
        fixed_pairs = []
        ru_pattern = re.compile(r'[а-яё]', flags=re.IGNORECASE)
        for lhs, rhs in pairs:
            if ru_pattern.search(lhs) and VKCrawler._define_language(rhs) == 'rus':
                raise ValueError(
                    f"There is a pair with the same languages: \n{lhs}\n{rhs}")
            if ru_pattern.search(lhs):
                fixed_pairs += [(lhs, rhs)]
            else:
                fixed_pairs += [(rhs, lhs)]
        return fixed_pairs

    @staticmethod
    def _define_language(text: str) -> str:
        if re.search(r'[\u4e00-\u9fff]+', text):
            return 'zho'
        elif re.search(r'[а-яё]', text, flags=re.IGNORECASE):
            return 'rus'
        return ''

    @staticmethod
    def _remove_trash(text: str) -> str:
        """ Remove 'ключевая фраза...' from the text.

        :param text: str, text to clear.
        :return: str, cleared text.
        """
        daily_remember = re.compile(
            r'(ключев(ая|ое) (фраза|слово|словосочетение)).*',
            flags=re.IGNORECASE
        )
        found = daily_remember.search(text)
        if found and found.group(0):
            start = text.index('\n\n')
            stop = text.index('\n\n', start + 2)
            removed = text[start:stop]

            if not daily_remember.search(removed):
                raise ValueError("Daily remember is not here")

            title = text[:start]
            txt = text[stop:]
            text = f"{title}\n\n{txt}"
        return text

    @staticmethod
    def _get_text(text: str) -> Sdict:
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
        hashtag_search = re.compile(
            r'#Новости_на_двух_языках', flags=re.IGNORECASE)
        assert hashtag_search.search(text), "There is no the hashtag"

        text = text.replace('\n \n', '\n\n')
        text = hashtag_search.sub('', text).strip()

        text = VKCrawler._remove_trash(text)

        paragraphs = text.split('\n')
        # remove hashtags
        paragraphs = filter(
            lambda txt: not txt.startswith('#'),
            paragraphs
        )
        paragraphs = [
            text.strip()
            for text in paragraphs
            if len(text) > 1
        ]

        if not paragraphs:
            raise ValueError("Post is empty")

        if len(paragraphs) % 2 is 1:
            raise ValueError("There is odd count of paragraphs")

        pairs = list(zip(paragraphs[::2], paragraphs[1::2]))
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

    def _format_date(self,
                     date: datetime.datetime) -> str:
        """ Format the date obj to str.

        :param date: datetime.date to format.
        :return: str, formatted date.
        """
        return date.strftime(self._dateformat)

    @staticmethod
    def _parse_post(post: Sdict,
                    dateformat: str) -> Sdict:
        """ Get all info from the post.

        Dict format {
            'title': (Russian, Chinese),
            'text': [(Russian, Chinese), (Russian, Chinese)...],
            'date': date, str format m/d/y
        }

        :param post: dict of str, post to parse.
        :param dateformat: str, dateformat.
        :return: dict of str with the format
        """
        date = VKCrawler._get_date(post['date'])
        date = date.strftime(dateformat)
        try:
            title_and_text = VKCrawler._get_text(post['text'])
        except AssertionError:
            raise
        except ValueError:
            raise

        return {
            **title_and_text,
            'date': date,
        }

    def _parse_posts(self) -> List[Sdict]:
        """ Parse all posts to dict format: {
           'header': header in Chinese
           'header_trans': header in Russian,
            'text': [(Russian, Chinese), (Russian, Chinese)...],
            'date': date, str format m/d/y
        }

        :return: list of parsed posts.
        """
        parsed_posts = []
        for post in self.posts:
            try:
                parsed_post = VKCrawler._parse_post(post, self._dateformat)
            except AssertionError as e:
                print(e, post, sep='\n', end='\n\n')
                self._skipped_posts += [post]
                continue
            except ValueError as e:
                print(e, post, sep='\n', end='\n\n')
                self._skipped_posts += [post]
                continue
            else:
                parsed_posts += [parsed_post]

        return parsed_posts

    @staticmethod
    def _dump_one(post: Sdict,
                  filepath: Path) -> None:
        """ Dump one post to the csv file.

        :param post: dict of str, post to dump.
        :param filepath: Path to the file.
        :return: None.
        """
        with filepath.open('w', newline='', encoding=ENCODING) as f:
            writer = csv.writer(
                f, delimiter=DELIMITER, quoting=csv.QUOTE_MINIMAL)
            for pair_ru_ch in post['text']:
                writer.writerow(pair_ru_ch)

    @staticmethod
    def _create_filename(title: str) -> Path:
        """ Remove wrong symbols from the title,
        replace spaces to the '_', add DATA_FOLDER
        as a parent and short the filename to 32 symbols.

        :param title: str, title of the post.
        :return: Path to the csv file in the DATA_FOLDER.
        """
        title = [
            symbol if symbol != ' ' else '_'
            for symbol in title
            if symbol.isalpha() or symbol == ' '
        ]
        filename = ''.join(title)[:32]
        return DATA_FOLDER / f"{filename}.csv"

    def dump_all(self) -> None:
        """ Dump to csv all posts and write to
        other file metadata.

        Filename is the first 32 symbols of
        Russian translation of the title.

        :return: None.
        """
        metadata = []
        for post in self.parsed_posts:
            header = post.pop('header')
            header_trans = post.pop('header_trans')
            date = post.pop('date')

            # name is the first 32 symbols of Russian
            # translation of the title
            path = VKCrawler._create_filename(header_trans)

            md = {
                'path': path.name,
                'header': header,
                'header_trans': header_trans,
                'created': date,
                'lang': 'zho',
                'lang_trans': 'rus'
            }
            metadata += [md]

            VKCrawler._dump_one(post, path)
        VKCrawler._dump_metadata(metadata)

    @staticmethod
    def _dump_one_skipped_post(post: Dict[str, Any],
                               path: Path,
                               dateformat: str) -> None:
        """ Dump skipped post to txt file.

        Dump only date in standard format and text.

        :param post: dict of str, post to dump.
        :param path: Path to txt the file.
        :param dateformat: str, dateformat.
        :return: None.
        """
        date = VKCrawler._get_date(post['date'])
        date = date.strftime(dateformat)
        with path.open('w', encoding=ENCODING) as f:
            f.write(f"{date}\n")
            f.write(post['text'])

    def dump_skipped_posts(self) -> None:
        """ Dump all skipped posts to separate
        files in the special folder.

        Their names are: 'post1.txt', 'post2.txt'...

        :return: None.
        """
        folder = self.data_folder / 'skipped_posts'
        os.makedirs(folder, exist_ok=True)

        for num, post in enumerate(self.skipped_posts, 1):
            path = folder / f"post{num}.txt"
            VKCrawler._dump_one_skipped_post(post, path, self._dateformat)

    @staticmethod
    def _dump_metadata(metadata: List[Sdict]) -> None:
        """ Dump metadata to the csv file, set an
        empty str to field if it does not exist.

        :param metadata: list of tuples, metadata to dump.
        :return: None.
        """
        path = DATA_FOLDER / 'metadata.csv'
        with path.open('w', newline='', encoding=ENCODING) as f:
            writer = csv.DictWriter(
                f, restval='',
                fieldnames=FIELDNAMES,
                delimiter=DELIMITER,
                quoting=csv.QUOTE_MINIMAL
            )
            writer.writeheader()
            writer.writerows(metadata)
