import os

from src.vk_crawler import VKCrawler

COMMUNITY_DOMAIN = 'cri_rus'


if __name__ == '__main__':
    access_token = os.environ.get('access_token')
    vk_crawler = VKCrawler(
        access_token,
        query='#Новости_на_двух_языках',
        domain=COMMUNITY_DOMAIN)
    # the last requested value – 891
