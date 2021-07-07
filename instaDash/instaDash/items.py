# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class InstadashItem(scrapy.Item):
    unique_id = scrapy.Field() # foreign_key 역할
    insta_id = scrapy.Field()
    username = scrapy.Field()
    follower = scrapy.Field()

    post_id = scrapy.Field()
    comments_count = scrapy.Field()
    likes_count = scrapy.Field()
    post_date = scrapy.Field()
