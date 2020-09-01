# Crawler

Собиратель постов для китайского параллельного подкорпуса в составе **НКРЯ**

### VKCrawler
* `__init__(access_token: str, method: str)`
    * `access_token` – токен доступа.
    * `method` – метод VK API, к которому происходит обращение.
    * Параметры, которые нужны для этого метода.
* `data_folder` – путь к папке с csv файлами с распаршенными постами.
* `skipped_posts_folder` – путь к папке с файлами, где хранятся посты, которые не получилось распарсить.
* `posts` – список словарей, ответы от VK API.
* `parsed_posts` – список словарей, распаршенные посты, ключи: `text`, `header`, `trans_header`, `date`.
* `skipped_posts` – список словарей, ответов от VK API, которые распарсить не получилось.
* `results_count` – количество вхождений запроса в VK.
* `url` – URL к VK Developers, объединённый с переданным методом. 
* `access_token` – переданный токен доступа.
* `update(last_results_count: int)` – получить новые посты, основываясь на предыдущем количестве результатов.
 `last_results_count` – предыдущее количество результатов.
* `request(count: int)` – запросить `count` результатов.
* `dump_all()` – записать распаршенные посты в csv файлы в `data_folder`. Также создать файл с метаинформацией.
* `dump_skipped_posts()` – записать посты, которые не получилось распарсить, в txt файлы в `skipped_posts_folder`.
* `from_txt_to_csv()` – вызывается после того, как файлы из `skipped_posts_folder` были приведены к верному формату.
Разбирает эти файлы, выгружает их в `parsed_posts`. Потом можно вызывать `dump_all()` для вывода их в csv.

#### Формат файлов
```text
month/day/year

title in Chinese
title in Russian

Paragraph in Chinese
Paragraph in Russian
```
В одном абзаце **одна строка**.
 

