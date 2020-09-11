import asyncio
from time import time
from typing import List

import aiofiles
import aiohttp
import bs4

P_NUM_STEP = 12
URL = "https://chuansongme.com/account/loveeyu"
CLEAN_URL = "https://chuansongme.com"
LINK_FILE = 'links.txt'


async def dump(links: List[str],
               filename: str) -> None:
    """ Dump links to file with tag 'a'.

    :param links: list of str, links to dump.
    :param filename: str, name of the file.
    :return: None.
    """
    async with aiofiles.open(filename, 'a', encoding='utf-8') as f:
        for link in links:
            await f.write(f"{link}\n")


def get_links(page: str) -> List[str]:
    """ Get links to the articles from the page.

    :param page: str, html code of the page.
    :return: list of str, links to articles from the page.
    """
    soup = bs4.BeautifulSoup(page, 'lxml')
    links = []
    for link in soup.find_all('a', {'class': 'question_link'}):
        try:
            link = link['href']
        except KeyError:
            print("Link is not found")
            continue
        full_link = f"{CLEAN_URL}{link}"
        links += [full_link]
    return links


async def fetch_coro(url: str,
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


async def bound_fetch(url: str,
                      start: int,
                      page_count: int) -> List[str]:
    """ Get pages code, parse links from there
    and dump them to the default file.

    :param url: str, url.
    :param page_count: int, count of pages.
    :return: list of str, dumped links.
    """
    async with aiohttp.ClientSession() as ses:
        tasks = [
            asyncio.create_task(fetch_coro(url, ses, start=P_NUM_STEP * mult))
            for mult in range(start, page_count + 1)
        ]
        pages_codes = []
        while True:
            done, pending = await asyncio.wait(tasks)
            for future in done:
                try:
                    page_code = future.result()
                    links = get_links(page_code)
                    await dump(links, LINK_FILE)
                except aiohttp.ClientResponseError as e:
                    print(e)

                    url_ = e.args[0].url
                    await dump([url_], 'skipped_urls1.txt')
                    continue
                except Exception as e:
                    print(e)
                    continue
                pages_codes += links
            return pages_codes


def get_page_codes(url: str,
                   start: int,
                   p_count: int) -> List[str]:
    return asyncio.run(bound_fetch(url, start, p_count))


if __name__ == '__main__':
    start = time()
    codes = get_page_codes(URL, 0, 100)
    assert len(codes) % 12 is 0
    print(f"Executing time {time() - start}")
    print(len(codes))
