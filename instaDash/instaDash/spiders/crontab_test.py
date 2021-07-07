import scrapy
from datetime import datetime, timedelta
import json
from ..middlewares import TooManyRequestsRetryMiddleware
from ..items import InstadashItem # items.py에서 정의한 클래스
from dateutil.relativedelta import relativedelta 
import pandas as pd
import time, os, re


class DbtestSpider(scrapy.Spider):
    
    name = "dbtest"

    PROFILE_URL = 'https://www.instagram.com/{}/?__a=1'
    POST_URL = 'https://www.instagram.com/graphql/query/?query_hash=02e14f6a7812a876f7d133c9555b1151&variables=\
                                            %7B%22id%22%3A%22{}%22%2C%22first%22%3A24%2C%22after%22%3A%22{}%22%7D'

    target_date = datetime.now() - relativedelta(months=1)

    # db에 insert 안된 아이디들
    user_names = ['yoona__lim', 'ggonekim', 'luv_ribbon', 'yerin_the_genuine', '_happiness_o', 'sunbin_eyemag', 'xxadoraa', 'sunye.m', 'd_a___m_i']
    
    cookies = {
        'csrftoken':'cQ84ypAxls5XqGEv9PLUpJfuqfcJdx6C',
        'ds_user_id':'1565063471',
        'sessionid':'1565063471%3AkAgJgVlJ5xQl2s%3A6',
    }

    def start_requests(self) :
        print('################## DB test 크롤링 시작 ! 현재시간 : ', datetime.now().strftime('%Y-%m-%d %H:%M'))
        for username in self.user_names:
            yield scrapy.Request(self.PROFILE_URL.format(username), callback=self.parse_profile, meta={'username':username}, cookies=DbtestSpider.cookies)

    # 프로필 + 포스트 첫번째 페이지
    def parse_profile(self, response):
        time.sleep(1)
        
        res = response.json()

        username = response.meta['username'] # ssoing
        unique_id = res['graphql']['user']['id'] # 241234513512
        full_name = res['graphql']['user']['full_name']
        follower = res['graphql']['user']['edge_followed_by']['count']

        edges = res['graphql']['user']['edge_owner_to_timeline_media']['edges']
        for edge in edges:
            node = edge['node']

            shortcode = node['shortcode']
            timestamp = node['taken_at_timestamp'] # timestamp 형식을 날짜-시간 형식으로 변환

            post_date = datetime.fromtimestamp(timestamp)
            post_date = post_date.strftime('%Y-%m-%d')
            
            comments_count = node['edge_media_to_comment']['count']
            likes_count = node['edge_media_preview_like']['count']

            # insta_items = SooingItem()
            # insta_items['unique_id'] = unique_id
            # insta_items['insta_id'] = username # str
            # insta_items['username'] = full_name # str
            # insta_items['follower'] = follower # int
            # insta_items['post_id'] = shortcode # str
            # insta_items['comments_count'] = comments_count # int
            # insta_items['likes_count'] = likes_count # int
            # insta_items['post_date'] = post_date # date

            yield {
                'unique_id': unique_id,
                'insta_id': username,
                'username': full_name,
                'follower': follower,
                'post_id': shortcode,
                'comments_count': comments_count,
                'likes_count': likes_count,
                'post_date': post_date,
            }

        end_cursor = res['graphql']["user"]["edge_owner_to_timeline_media"]["page_info"]["end_cursor"]

        yield scrapy.Request(self.POST_URL.format(unique_id, end_cursor), callback=self.parse_posts, meta={'unique_id': unique_id, 'username': username, 'full_name': full_name, 'follower': follower}, cookies=DbtestSpider.cookies)

    def parse_posts(self, response):
        time.sleep(1)
        unique_id = response.meta['unique_id']
        username = response.meta['username']
        full_name = response.meta['full_name']
        follower = response.meta['follower']

        res = response.json()
        edges = res['data']['user']['edge_owner_to_timeline_media']['edges']
        for edge in edges:
            node = edge['node']

            shortcode = node['shortcode']
            timestamp = node['taken_at_timestamp'] # timestamp 형식을 날짜-시간 형식으로 변환

            post_date = datetime.fromtimestamp(timestamp)
            if post_date < self.target_date: # 게시물의 업로드 날짜가 지난 한달 동안의 범위를 벗어날 경우
                return
            post_date = post_date.strftime('%Y-%m-%d')

            comments_count = node['edge_media_to_comment']['count']
            likes_count = node['edge_media_preview_like']['count']

            # insta_items = SooingItem()
            # insta_items['unique_id'] = unique_id
            # insta_items['insta_id'] = username # str
            # insta_items['username'] = full_name # str
            # insta_items['follower'] = follower # int
            # insta_items['post_id'] = shortcode # str
            # insta_items['comments_count'] = comments_count # int
            # insta_items['likes_count'] = likes_count # int
            # insta_items['post_date'] = post_date # date

            yield {
                'unique_id': unique_id,
                'insta_id': username,
                'username': full_name,
                'follower': follower,
                'post_id': shortcode,
                'comments_count': comments_count,
                'likes_count': likes_count,
                'post_date': post_date,
            }