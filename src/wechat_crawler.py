import asyncio
import csv
from pathlib import Path
from typing import List, Tuple

import aiohttp
import bs4

P_NUM_STEP = 12
URL = "https://chuansongme.com/account/loveeyu"
BASE_URL = "https://chuansongme.com"
LINK_FILE = 'links.txt'

TEMPLATE_FILENAME = "cri_rus_2020_wechat{}.csv"
METADATA_PATH = Path(f"meta_{TEMPLATE_FILENAME.format('')}")

# for csv writing
DELIMITER = '\t'
FIELDNAMES = (
    'path', 'header', 'created', 'author', 'birthday', 'header_trans',
    'author_trans', 'translator', 'date_trans', 'sphere', 'lang', 'lang_trans'
)
ARTICLE_NUM = 1


async def get_html_coro(url: str,
                        ses: aiohttp.ClientSession,
                        **kwargs) -> str:
    """ Get the html code of the page.

    There is ClientTimeout = 30s.

    Raise an exception if something went wrong.

    :param url: str, page url.
    :param ses: aiohttp.ClientSession.
    :param kwargs: html tags.
    :return: str, html code of the page, decoded to UTF-8.
    """
    timeout = aiohttp.ClientTimeout(30)
    async with ses.get(url, params=kwargs, timeout=timeout) as resp:
        if resp.status is 200:
            await asyncio.sleep(2)
            return await resp.text()
        resp.raise_for_status()


async def bound_fetch(urls: List[str]) -> List[str]:
    """ Get pages code.

    :param urls: list of str, urls to get their html codes.
    :return: list of str, html codes of pages.
    """
    async with aiohttp.ClientSession() as ses:
        tasks = [
            asyncio.create_task(get_html_coro(url, ses))
            for url in urls
        ]
        pages_codes = []
        while True:
            done, pending = await asyncio.wait(tasks)
            for future in done:
                try:
                    page_code = future.result()
                except Exception as e:
                    print(e)
                else:
                    pages_codes += [page_code]
            return pages_codes


def get_page_codes(urls: List[str]) -> List[str]:
    return asyncio.run(bound_fetch(urls))


def valid_articles(base_soup: bs4.BeautifulSoup):
    """ Generator to get valid articles.
    Means there is demanded marker in the title.

    :param base_soup: bs4.BeautifulSoup, page soup.
    :return: article soup.
    """
    for link in base_soup.find_all('a', {'class': 'question_link'}):
        full_link = f"{BASE_URL}{link}"
        html = requests.get(full_link).text
        soup = bs4.BeautifulSoup(html, 'lxml')

        title = soup.find()
        if '导言' in title:
            yield soup


def parse_article(article: bs4.BeautifulSoup) -> List[Tuple[str, str]]:
    """ Get data and metadata from the article.

    :param article: bs4.BeautifulSoup, article to parse.
    :return: tuple of data (list of tuples: (Russian, Chinese)...)
    and metadata (dict).
    """
    pass


def dump_article(article: List[Tuple[str, str]],
                 filepath: Path) -> None:
    with filepath.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(
            f, delimiter=DELIMITER, quoting=csv.QUOTE_MINIMAL)
        for pair_ru_ch in article:
            writer.writerow(pair_ru_ch)


def dump_metadata(metadata: dict) -> None:
    with METADATA_PATH.open('a', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(
            f, fieldnames=FIELDNAMES,
            delimiter=DELIMITER,
            quoting=csv.QUOTE_MINIMAL)

        # TODO
        # writer.writeheader()
        writer.writerow(metadata)


def main():
    # TODO: идти по 100 за заход
    urls = [
        f"{URL}?start={P_NUM_STEP * mult}"
        for mult in range(994)
    ]

    for num, url in enumerate(urls, 1):
        print(num, 'page in processing...')
        filepath = Path(TEMPLATE_FILENAME.format(num))

        html = requests.get(url)
        soup = bs4.BeautifulSoup(html, 'lxml')
        for article in valid_articles(soup):
            parsed_article, md = parse_article(article)

            dump_article(parsed_article, filepath)
            dump_metadata(md)


if __name__ == '__main__':
    main()