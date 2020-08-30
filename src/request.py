import asyncio
from typing import Dict, Any, List, Tuple

import aiohttp

# timeout
WAIT = 25
# Dict[str, Any]
Sdict = Dict[str, Any]


async def get_json_coro(url: str,
                        ses: aiohttp.ClientSession,
                        **params) -> Tuple[int, Sdict]:
    """ Coro, requesting to the URL with params
    and getting json from there.

    There is ClientTimeout.

    :param url: str, URL to request.
    :param ses: aiohttp.ClientSession.
    :param params: dict of str, HTTP tags.
    :return: json dict decoded to UTF-8.
    :exception: if sth went wrong.
    """
    timeout = aiohttp.ClientTimeout(WAIT)
    order = params.pop('order')
    async with ses.get(
            url, timeout=timeout, params=params) as resp:
        if resp.status is 200:
            return order, await resp.json(encoding='utf-8')
        resp.raise_for_status()


async def bound_fetch(url: str,
                      params: Sdict) -> List[Sdict]:
    """ Coro, getting json with get_json_coro
    and catching exceptions.

    :param url: str, URL to request.
    :param params: dict of str, HTTP tags.
    :return: json dict decoded to UTF-8.
    :exception: if sth went wrong.
    """
    async with aiohttp.ClientSession() as ses:
        count = params.pop('count', 0)
        remains = count
        tasks = []
        # this number is needed to save the order
        for num, offset in enumerate(range(0, count, 100)):
            if count > remains:
                count = remains
            if count > 100:
                count = 100

            task = asyncio.create_task(
                    get_json_coro(
                        url, ses, **params,
                        offset=offset, count=count, order=num
                    )
                )
            tasks += [task]
            remains -= count

        if not tasks:
            raise ValueError(f"Wrong count given: {count}")
        posts = []
        while True:
            done, pending = await asyncio.wait(tasks)
            for future in done:
                try:
                    json_dict = future.result()
                except Exception:
                    raise
                else:
                    posts += [json_dict]
            # the order is sorted for ability to get
            # some new posts excluding old ones
            posts.sort(key=lambda x: x[0])
            return [
                enumerated_post[1]
                for enumerated_post in posts
            ]


def get_json(url: str,
             **params) -> Sdict:
    """ Request to the URL with params asynchronously
    and get json decoded to UTF-8 from there.

    :param url: str, URL to request.
    :param params: dict of str, HTTP tags.
    :return: json dict decoded to UTF-8.
    :exception: if sth went wrong.
    """
    return asyncio.run(bound_fetch(url, params))
