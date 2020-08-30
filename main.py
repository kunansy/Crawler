import asyncio
import csv
import datetime
import os
from pathlib import Path
from typing import Dict, Any, Tuple, List

import aiohttp

BASE_URL = "https://api.vk.com/method/"
SEARCH_ON_WALL = "wall.search"

COMMUNITY_DOMAIN = 'cri_rus'
# timeout
WAIT = 25

# folder with csv files
DATA_FOLDER = Path('data')
os.makedirs(DATA_FOLDER, exist_ok=True)


async def get_json_coro(url: str,
                        ses: aiohttp.ClientSession,
                        params: Dict[str, Any]) -> Dict[str, Any]:
    """ Coro, requesting to the url with params
    and getting json from there.

    There is ClientTimeout.

    :param url: str, url to request.
    :param ses: aiohttp.ClientSession.
    :param params: dict of str, HTTP tags.
    :return: json dict decoded to UTF-8.
    :exception: if sth went wrong.
    """
    timeout = aiohttp.ClientTimeout(WAIT)
    async with ses.get(url, timeout=timeout, params=params) as resp:
        if resp.status is 200:
            return await resp.json(encoding='utf-8')
        resp.raise_for_status()


async def bound_fetch(url: str,
                      params: Dict[str, Any]) -> Dict[str, Any]:
    """ Coro, getting json with get_json_coro
    and catching exceptions.

    :param url: str, url to request.
    :param params: dict of str, HTTP tags.
    :return: json dict decoded to UTF-8.
    :exception: if sth went wrong.
    """
    async with aiohttp.ClientSession() as ses:
        tasks = [
            asyncio.create_task(get_json_coro(url, ses, params))
        ]
        while True:
            done, pending = await asyncio.wait(tasks)
            for future in done:
                try:
                    json_dict = future.result()
                except Exception:
                    raise
                return json_dict


def get_json(url: str,
             params: Dict[str, Any]) -> Dict[str, Any]:
    """ Request to the url with params asynchronously
    and get json decoded to UTF-8 from there.

    :param url: str, url to request.
    :param params: dict of str, HTTP tags.
    :return: json dict decoded to UTF-8..
    :exception: if sth went wrong.
    """
    return asyncio.run(bound_fetch(url, params))


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
        return f"{self._url}/{self._method}"

    @property
    def posts(self) -> List[Dict[str, Any]]:
        return self._posts

    @property
    def parsed_posts(self) -> List[Dict[str, Any]]:
        return self._parsed_posts

    @property
    def access_token(self) -> str:
        return self._access_token

    def request(self,
                count: int) -> None:
        posts = get_json(self.url, **self._params, count=count)
        posts = [
            item['response']['items']
            for item in posts
        ]
        self._posts = sum(posts, [])
        self._parsed_posts = self._parse_posts()

    @staticmethod
    def _get_text(text: str) -> Dict[str, Any]:
        """ Parse text to its title and list of tuples:
        Chinese text, Russian text.

        Dict format {
            'title': (Chinese, Russian),
            'text': [(Chinese, Russian), (Chinese, Russian)...]
        }

        :param text: str, text to parse.
        :return: dict of str with the format.
        """
        patter = re.compile(
            r'#Новости_на_двух_языках', flags=re.IGNORECASE)
        assert patter.search(text), "There is no the hashtag"

        # here is '#Новости_на_двух_языках', remove it
        paragraphs = text.split('\n')[2:]

        # there is '。' at the end of the Chinese text
        paragraphs = [
            text.replace('。', '').strip()
            for text in paragraphs
            if len(text) > 1
        ]

        title = paragraphs[0], paragraphs[1]
        # exclude the title from here
        pairs = list(zip(paragraphs[2::2], paragraphs[3::2]))
        return {
            'title': title,
            'text': pairs
        }

    @staticmethod
    def _get_date(timestamp: int) -> datetime.datetime:
        """ Convert date from timestamp to datetime.

        :param timestamp: int, date in timestamp.
        :return: datetime.
        """
        return datetime.datetime.fromtimestamp(timestamp)

    @staticmethod
    def _parse_post(post: Dict[str, Any]) -> Dict[str, Any]:
        """ Get all info from the post.

        Dict format {
            'title': (Chinese, Russian),
            'text': [(Chinese, Russian), (Chinese, Russian)...],
            'date': datetime
        }

        :param post: dict of str, post to parse.
        :return: dict of str with the format
        """
        date = VKCrawler._get_date(post['date'])
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
           'title': (Chinese, Russian),
            'text': [(Chinese, Russian), (Chinese, Russian)...],
            'date': datetime
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
            title = post.pop('title')
            date = post.pop('date')
            # name is the first 16 symbols of Russian
            # translation of the title
            path = VKCrawler._create_filename(title[1])
            metadata += [(path.name, *title, date)]

            VKCrawler._dump_one(post, path)
        VKCrawler._dump_metadata(metadata)

    @staticmethod
    def _dump_metadata(metadata: List[Tuple[Any]]) -> None:
        """ Dump metadata to the csv file.

        :param metadata: list of tuples, metadata to dump.
        :return: None.
        """
        path = DATA_FOLDER / 'metadata.csv'
        with path.open('w', newline='', encoding='utf-8') as f:
            writer = csv.writer(
                f, delimiter=DELIMITER, quoting=csv.QUOTE_MINIMAL)

            writer.writerows(metadata)


if __name__ == '__main__':
    url = f"{BASE_URL}/{SEARCH_ON_WALL}"
    access_token = os.environ.get('access_token')
    params = {
        'domain': COMMUNITY_DOMAIN,
        'v': '5.122',
        'query': '#Новости_на_двух_языках',
        'access_token': access_token,
        'count': 10
    }

    result = get_json(url, params)

    posts = result['response']['items']
    parsed_posts = parse_posts(posts)
    dump_all(parsed_posts)

