import os

from .vk_crawler import VKCrawler

COMMUNITY_DOMAIN = 'cri_rus'


if __name__ == '__main__':
    access_token = os.environ.get('access_token')
    vk_crawler = VKCrawler(
        access_token,
        query='#Новости_на_двух_языках',
        domain=COMMUNITY_DOMAIN)
    vk_crawler.request(250)
    for post in vk_crawler.posts:
        print(post)

    print(len(vk_crawler.parsed_posts))
