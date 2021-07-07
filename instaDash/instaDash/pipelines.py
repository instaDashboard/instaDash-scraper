# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import MySQLdb

class InstadashPipeline:
    host = 'sooing-db.c5flk7kuctds.ap-northeast-2.rds.amazonaws.com'
    user = 'sooing'
    password = 'Sooingdb0926!'
    db_name = 'sooing_db' # 스키마 이름

    def __init__(self):
        self.connection = MySQLdb.connect(self.host,
                                        self.user,
                                        self.password,
                                        self.db_name,
                                        charset="utf8mb4",
                                        )
        self.cursor = self.connection.cursor()
        print("DB 정상 접속.")

    def process_item(self, item, spider):
        # query문 때문에 insta id가 덜 들어가는 건지..?
        try:
            query_influencer = """INSERT INTO influencer (unique_id, insta_id, username, follower) VALUES ("{}", "{}", "{}", "{}") ON DUPLICATE KEY UPDATE
                                    unique_id="{}", insta_id="{}", username="{}", follower="{}"
                                """.format(
                item['unique_id'], item['insta_id'], item['username'], item['follower'], 
                item['unique_id'], item['insta_id'], item['username'], item['follower']
                )

            self.cursor.execute(query_influencer)
            self.connection.commit() # 여기서도 하고 아래에서도 하면 중복커밋이 될지..??

            query_post = """INSERT INTO post (unique_id, post_id, comments_count, likes_count, post_date) VALUES ("{}", "{}", "{}", "{}", "{}") ON DUPLICATE KEY UPDATE
                                    comments_count="{}", likes_count="{}"
                                """.format(
                item['unique_id'], item['post_id'], item['comments_count'], item['likes_count'], item['post_date'], 
                item['comments_count'], item['likes_count']
                )

            self.cursor.execute(query_post)
            self.connection.commit()
            print(f"{item['insta_id']}의 데이터가 정상적으로 INSERT 되었습니다.")

        except MySQLdb.Error as e:
            print("Error {}: {}".format(e.args[0], e.args[1]))
            return item