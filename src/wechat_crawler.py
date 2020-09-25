import asyncio
import csv
from pathlib import Path
from typing import List, Tuple

import aiohttp
import bs4

P_NUM_STEP = 12
URL = "https://chuansongme.com/account/loveeyu"
BASE_URL = "https://chuansongme.com"

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
                    # TODO: logger
                    print(e)
                else:
                    pages_codes += [page_code]
            return pages_codes


def get_page_codes(urls: List[str]) -> List[str]:
    return asyncio.run(bound_fetch(urls))


def valid_articles(page_code: str):
    """ Generator to get valid articles.
    Means there is demanded marker in the title.

    Get articles codes, chick their valid,
    yield one if it is.

    :param page_code: str, page html code.
    :return: yield article soup.
    """
    soup = bs4.BeautifulSoup(page_code, 'lxml')
    links = soup.find_all('a', {'class': 'question_link'})
    assert len(links) is 12, "Wrong links count, 12 expected"

    for step in range(3):
        # there are 12 articles on a page,
        # divide them to blocks by 3
        block = links[step: 3 + step]
        full_links = [
            f"{BASE_URL}{link['href']}"
            for link in block
        ]

        article_codes = get_page_codes(full_links)
        for article_code, article_url in zip(article_codes, full_links):
            soup = bs4.BeautifulSoup(article_code, 'lxml')
            title = soup.find('section', {'data-brushtype': 'text'})
            try:
                title = title.text
            except AttributeError as e:
                # TODO: logger
                print(f"Article has no title, URL: {article_url}")
                continue
            if '导言' in title or '导 言' in title:
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
    """ Dump the parsed article to the file.

    :param article: list of tuples to dump [(Russian, Chinese)...].
    :param filepath: Path to the file to dump.
    :return: None.
    """
    with filepath.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(
            f, delimiter=DELIMITER, quoting=csv.QUOTE_MINIMAL)
        for pair_ru_ch in article:
            writer.writerow(pair_ru_ch)


def dump_metadata(metadata: dict) -> None:
    """ Dump metadata to the file, update it.

    :param metadata: dict of str, metadata to dump.
    :return: None.
    """
    with METADATA_PATH.open('a', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(
            f, fieldnames=FIELDNAMES,
            delimiter=DELIMITER,
            quoting=csv.QUOTE_MINIMAL)

        # TODO
        # writer.writeheader()
        writer.writerow(metadata)


def parse_page(page_code: str) -> None:
    """ Get valid articles from the page, parse them,
    dump them to the files and dump metadata.

    :param page_code: str, html code of the page to parse.
    :return: None.
    """
    global ARTICLE_NUM
    for article in valid_articles(page_code):
        print(ARTICLE_NUM, "article in process...")
        filepath = Path(TEMPLATE_FILENAME.format(ARTICLE_NUM))

        # get pairs [(Russian, Chinese)...]
        # and metadata from the article
        parsed_article, md = parse_article(article)
        # dump article to the file
        dump_article(parsed_article, filepath)
        # dump metadata to the file, update it
        dump_metadata(md)

        ARTICLE_NUM += 1


def parse_block(start: int,
                stop: int) -> None:
    """ Parse a block of 5 pages: get valid
    articles and dump them. Also dump metadata.

    :param start: int, range start.
    :param stop: int, range stop.
    :return: None.
    """
    global ARTICLE_NUM
    urls = [
        f"{URL}?start={P_NUM_STEP * mult}"
        for mult in range(start, stop)
    ]
    assert len(urls) is 5, "Wrong len, 5 expected"

    page_codes = get_page_codes(urls)
    for page_code in page_codes:
        parse_page(page_code)


if __name__ == '__main__':
    parse_block(0, 5)
