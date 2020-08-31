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
os.makedirs(DATA_FOLDER, exist_ok=True)

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

# some texts contains strings which are noe expected
# in parsed posts but it is impossible to remove
# them automatically. This list is needed for this.
BLACK_LIST = ['huāchē xúnyóu']


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
        for lhs, rhs in pairs:
            if VKCrawler._define_language(lhs) == VKCrawler._define_language(rhs):
                raise ValueError(
                    f"There is a pair with the same languages: {lhs}\n{rhs}")
            if VKCrawler._define_language(lhs) == 'rus':
                fixed_pairs += [(lhs, rhs)]
            else:
                fixed_pairs += [(rhs, lhs)]
        return [
            (ru, ch)
            for ch, ru in pairs
        ]

    @staticmethod
    def _define_language(text: str) -> str:
        if re.search(r'[\u4e00-\u9fff]+', text):
            return 'zho'
        elif re.search(r'[а-яё]', text, flags=re.IGNORECASE):
            return 'rus'
        return ''

    @staticmethod
    def _remove_hint_to_learn(paragraphs: List[str]) -> List[str]:
        """ Remove 'Ключевая фраза на сегодня'
        and all its components (1) from the text.

        (1):

        1. Phrase 'Ключевая фраза на сегодня'.

        2. Its Chinese version (two types).

        3. The phrase on the day (Russian and Chinese).

        4. Transcription.

        :param paragraphs: list of str,
        """
        if 'Ключевая фраза на сегодня:' not in paragraphs:
            return paragraphs

        # for debug
        deleted = []

        ru_index = paragraphs.index('Ключевая фраза на сегодня:')
        step_index = ru_index

        in_ch = '今日关键词'
        if '每日关键词' in paragraphs:
            in_ch = '每日关键词'
        elif '今日关键词：' in paragraphs:
            in_ch = '今日关键词：'

        if in_ch in paragraphs:
            ch_index = paragraphs.index(in_ch)
            # delete only if these symbols
            # are not in the main text
            if ch_index == ru_index + 1:
                deleted += [paragraphs[ch_index]]
                del paragraphs[ch_index]
            elif ch_index == ru_index - 1:
                deleted += [paragraphs[ch_index]]
                del paragraphs[ch_index]
                step_index -= 1
                ru_index -= 1
            else:
                print(f"'{in_ch}' these symbols were expected "
                      "to be after or before the 'Ключевая фраза на сегодня:',"
                      "but there are not")

        deleted += [paragraphs[ru_index]]
        del paragraphs[ru_index]

        # if there is a Chinese and Russian symbol here
        # mens there are original and translation texts
        # delete only this str
        if (re.search(r'[\u4e00-\u9fff]+', paragraphs[step_index]) and
                re.search(r'[а-яёА-яЁ]', paragraphs[step_index])):
            deleted += [paragraphs[step_index]]
            del paragraphs[step_index]
        else:
            l1 = VKCrawler._define_language(paragraphs[step_index])
            l2 = VKCrawler._define_language(paragraphs[step_index + 1])

            # Chinese and Russian translation
            # are in the different strings
            if l1 and l2 and l1 != l2:
                deleted += [paragraphs[step_index]]
                deleted += [paragraphs[step_index + 1]]
                del paragraphs[step_index]
                del paragraphs[step_index]
            elif l1 == l2:
                # WTF: there is no translation of it!
                deleted += [paragraphs[step_index]]
                del paragraphs[step_index]

        # if there is a transcription
        if (re.search(r'^\[.+\]$', paragraphs[step_index]) or
                paragraphs[step_index] in BLACK_LIST):
            deleted += [paragraphs[step_index]]
            del paragraphs[step_index]

        deleted = '\n'.join(
            f"{num}. {sent}"
            for num, sent in enumerate(deleted, 1)
        )
        # remains = '\n'.join(
        #     f"{num}. {sent}"
        #     for num, sent in enumerate(paragraphs[ru_index - 2: ru_index + 5], 1)
        # )
        print(f"Deleted: \n{deleted}", end='\n\n')
        # print(f"Remains: \n{remains}", end='\n\n')

        return paragraphs

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

    @staticmethod
    def _parse_post(post: Sdict) -> Sdict:
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
                parsed_post = VKCrawler._parse_post(post)
            except AssertionError as e:
                print(e, post, sep='\n')
                continue
            except ValueError as e:
                print(e, post, sep='\n')
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
            for pair_ch_ru in post['text']:
                writer.writerow(pair_ch_ru)

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
