from ..middlewares import TooManyRequestsRetryMiddleware
from ..items import InstadashItem
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta 
import pandas as pd
import scrapy, json, time, os, pickle, re

# Insta Cralwer
class InstaSpider(scrapy.Spider):
    
    name = "insta"

    PROFILE_URL = 'https://www.instagram.com/{}/?__a=1'

    POST_URL = 'https://www.instagram.com/graphql/query/?query_hash=02e14f6a7812a876f7d133c9555b1151&variables=\
                                            %7B%22id%22%3A%22{}%22%2C%22first%22%3A24%2C%22after%22%3A%22{}%22%7D'

    target_date = datetime.now() - relativedelta(months=1)

    # follower_file_count = len(os.listdir('/home/ubuntu/workspace/sooing/sooing/datas/followers'))
    # post_file_count = len(os.listdir('/home/ubuntu/workspace/sooing/sooing/datas/posts'))

    user_names = list(pd.read_csv('/home/ubuntu/workspace/instaDash/influencer_re.csv')['username'])

    cookies = {
        'csrftoken':'cQ84ypAxls5XqGEv9PLUpJfuqfcJdx6C',
        'ds_user_id':'1565063471',
        'sessionid':'1565063471%3AkAgJgVlJ5xQl2s%3A6',
    }

    def start_requests(self) :
        print('################## 크롤링 시작! 현재시간 : ', datetime.now().strftime('%Y-%m-%d %H:%M'))
        for username in self.user_names:
            time.sleep(1)
            yield scrapy.Request(self.PROFILE_URL.format(username), callback=self.parse_profile, meta={'username':username}, cookies=InstaSpider.cookies)

    # 프로필 + 포스트 첫번째 페이지
    def parse_profile(self, response):
        time.sleep(1)
        
        res = response.json()
        username = response.meta['username'] # ssoing
        unique_id = res['graphql']['user']['id'] # 241234513512
        full_name = res['graphql']['user']['full_name']
        follower = res['graphql']['user']['edge_followed_by']['count']

        # with open('/home/ubuntu/workspace/sooing/sooing/datas/followers/user_info_%s.txt'%InstaSpider.follower_file_count, 'a') as f:
        #     f.write('\t'.join((username,str(unique_id),full_name,str(follower))))
        #     f.write('\n')

        edges = res['graphql']['user']['edge_owner_to_timeline_media']['edges']
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

            # with open('/home/ubuntu/workspace/sooing/sooing/datas/posts/user_post_%s.txt'%InstaSpider.post_file_count, 'a') as f:
            #     f.write('\t'.join((str(unique_id), shortcode, str(comments_count), str(likes_count), post_date)))
            #     f.write('\n')

            insta_items = SooingItem()
            insta_items['unique_id'] = unique_id
            insta_items['insta_id'] = username # str
            insta_items['username'] = full_name # str
            insta_items['follower'] = follower # int
            insta_items['post_id'] = shortcode # str
            insta_items['comments_count'] = comments_count # int
            insta_items['likes_count'] = likes_count # int
            insta_items['post_date'] = post_date # date

            yield insta_items

        end_cursor = res['graphql']["user"]["edge_owner_to_timeline_media"]["page_info"]["end_cursor"]

        # 두번째 페이지로 이동하기 위한 리퀘스트
        yield scrapy.Request(self.POST_URL.format(unique_id, end_cursor), callback=self.parse_posts, meta={'unique_id': unique_id, 'username': username, 'full_name': full_name, 'follower': follower}, cookies=InstaSpider.cookies)


    # 포스트 두번째~마지막페이지(12개씩)
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

            # with open('/home/ubuntu/workspace/sooing/sooing/datas/posts/user_post_%s.txt'%InstaSpider.post_file_count, 'a') as f:
            #     f.write('\t'.join((str(unique_id), shortcode, str(comments_count), str(likes_count), post_date)))
            #     f.write('\n')

            insta_items = SooingItem()
            insta_items['unique_id'] = unique_id
            insta_items['insta_id'] = username # str
            insta_items['username'] = full_name # str
            insta_items['follower'] = follower # int
            insta_items['post_id'] = shortcode # str
            insta_items['comments_count'] = comments_count # int
            insta_items['likes_count'] = likes_count # int
            insta_items['post_date'] = post_date # date

            yield insta_items

        end_cursor = res['data']["user"]["edge_owner_to_timeline_media"]["page_info"]["end_cursor"]

        if end_cursor:
            yield scrapy.Request(self.POST_URL.format(unique_id, end_cursor), callback=self.parse_posts, meta={'unique_id': unique_id, 'username': username, 'full_name': full_name, 'follower': follower}, cookies=InstaSpider.cookies)