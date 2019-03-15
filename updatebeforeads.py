
from pymongo import MongoClient
import time
import pymysql
import pandas as pd
import copy
import os
import re
import pytz
import uuid
import datetime
import logging
from pytz import timezone, utc


tz = pytz.timezone('Asia/Shanghai')

def custom_time(*args):
    # 配置logger
    utc_dt = utc.localize(datetime.datetime.utcnow())
    my_tz = timezone("Asia/Shanghai")
    converted = utc_dt.astimezone(my_tz)
    return converted.timetuple()


logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
if os.path.exists(r'./logs') == False:
    os.mkdir('./logs')
    if os.path.exists('./updatebefore_log.txt') == False:
        fp = open("./logs/updatebefore_log.txt", 'w')
        fp.close()
handler = logging.FileHandler("./logs/updatebefore_log.txt", encoding="UTF-8")
handler.setLevel(logging.INFO)
logging.Formatter.converter = custom_time
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class UpdateBeforeAds:

    def __init__(self):
        self.db_host = os.environ['db_host']
        self.db_name = os.environ['db_name']
        self.db_port = int(os.environ['db_port'])
        self.db_user = os.environ['db_user']
        self.db_pwd = os.environ['db_pwd']
        self.db_report_name = os.environ['db_report_name']
        self.db_ads_name = os.environ['db_ads_name']
        self.mysql_db_host = os.environ['mysql_db_host']
        self.mysql_db_port = int(os.environ['mysql_db_port'])
        self.mysql_db_user = os.environ['mysql_db_user']
        self.mysql_db_pwd = os.environ['mysql_db_pwd']
        self.mysql_db_name = os.environ['mysql_db_name']
        self.client = None
        self.db = None
        self.interests = dict()
        self.behaviors = dict()
        self.pts = list()
        self.urls = []

    def mongodb_conn(self):
        client = MongoClient(host=self.db_host, port=self.db_port)
        db = client.get_database(self.db_name)
        db.authenticate(self.db_user.strip(), self.db_pwd.strip())
        self.client = client
        self.db = db

    def mysql_connection(self):
        self.mysql_conn = pymysql.connect(host=self.mysql_db_host, port=self.mysql_db_port, user=self.mysql_db_user,
                                          passwd=self.mysql_db_pwd, db=self.mysql_db_name, charset='utf8')
        self.mysql_cursor = self.mysql_conn.cursor()

    def close(self):
        self.client.close()

    def select_behaviors(self):
        conn = pymysql.connect(host=self.mysql_db_host, user=self.mysql_db_user, password=self.mysql_db_pwd,
                               db=self.mysql_db_name, port=self.mysql_db_port)
        sql = 'select id from dw_dim_behavior'
        df_ids = pd.read_sql(sql, conn)
        for index in range(len(df_ids)):
            row = df_ids.iloc[index]
            self.behaviors[row['id']] = 1
        conn.close()

    def select_interests(self):
        conn = pymysql.connect(host=self.mysql_db_host, user=self.mysql_db_user, password=self.mysql_db_pwd,
                               db=self.mysql_db_name, port=self.mysql_db_port)
        sql = 'select id from dw_dim_interest'
        df_ids = pd.read_sql(sql, conn)
        for index in range(len(df_ids)):
            row = df_ids.iloc[index]
            self.interests[row['id']] = 1
        conn.close()

    def select_url(self):
        conn = pymysql.connect(host=self.mysql_db_host, user=self.mysql_db_user, password=self.mysql_db_pwd,
                               db=self.mysql_db_name, port=self.mysql_db_port)
        sql = 'select distinct a.videoId,a.urlThumbnail,b.message1,b.message2,b.message3,b.message4  from ' \
              'dw_dim_creative_media a join(select * from dw_dim_creative_text)b on a.videoId=b.videoId'
        urls = pd.read_sql(sql, conn)
        for index in range(len(urls)):
            row = urls.iloc[index]
            self.urls.append({'videoId': row['videoId'],
                              'urlThumbnail': row['urlThumbnail'],
                              'message1': row['message1'],
                              'message2': row['message2'],
                              'message3': row['message3'],
                              'message4': row['message4']
                              })
        print(len(self.urls))

    def select_mysql(self):
        self.select_behaviors()
        self.select_interests()
        self.select_url()
        # print(len(self.interests))
        # print(self.interests)
        # print(len(self.behaviors))
        # print(self.behaviors)

    def modify_pt(self, delt_name, platform, country, pt, creative_media):
        cur_date = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
        new_name = re.subn(r"(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2})", cur_date, pt['name'])[0]
        # tmp_sp = new_name.split()
        # tmp_sp[0] = delt_name.upper()
        # tmp_sp[3] = platform.upper()
        # tmp_sp[4] = country.upper()
        # new_name = ' '.join(tmp_sp)
        new_name = re.sub(r'\w* ', (delt_name+' ').upper(), new_name, count=1, flags=0)
        new_name = new_name.replace('[GA2]', '').replace('[GA1]', '').replace('[GA]', '').strip()
        pt['name'] = new_name
        pt['adset_spec']['name'] = new_name
        pt['adset_spec']['campaign_spec']['name'] = new_name

        # print(delt_name, ':', new_name)

        ## 修改images_url
        if pt.get('creative') and pt['creative'].get('object_story_spec') and \
                pt['creative']['object_story_spec'].get('video_data') and \
                pt['creative']['object_story_spec']['video_data'].get('image_hash'):
            del pt['creative']['object_story_spec']['video_data']['image_hash']
        pt['creative']['object_story_spec']['video_data']['image_url'] = creative_media['urlThumbnail']
        message = creative_media['message1']
        if creative_media['message4']:
            message = creative_media['message4']
        elif creative_media['message3']:
            message = creative_media['message3']
        elif creative_media['message2']:
            message = creative_media['message2']

        pt['creative']['object_story_spec']['video_data']['message'] = message
        pt['creative']['object_story_spec']['video_data']['videoId'] = creative_media['videoId']
        return pt

    def find_belt_name(self, belt_name):
        tmp_belt_name = belt_name.split('_')
        country = None
        platform = None
        if tmp_belt_name[1].upper() == 'IOS':
            country = tmp_belt_name[2].upper()
            platform = 'iOS'
        elif tmp_belt_name[2].upper() == 'IOS':
            country = tmp_belt_name[1].upper()
            platform = 'iOS'
        elif tmp_belt_name[1].upper() == 'ADR' or tmp_belt_name[1].upper() == 'ANDROID':
            country = tmp_belt_name[2].upper()
            platform = 'Android'
        elif tmp_belt_name[2].upper() == 'ADR' or tmp_belt_name[2].upper() == 'ANDROID':
            country = tmp_belt_name[1].upper()
            platform = 'Android'
        if country and platform:
            colles_delivery = self.db.delivery.find({'country': country, 'platform': platform}, {'_id': 0, 'name': 1,
                                                                                                 'country': 1,
                                                                                                 'platform': 1
                                                                                                 })
            colles_delivery = list(colles_delivery)
            return colles_delivery
        return list()

    def find_ads(self, ad_ids):
        colles_ads = self.db.get_collection(self.db_ads_name).find({'ad_id': {'$in': ad_ids}},
                                                                   {'_id': 0, 'delt_name': 1, 'pt': 1})
        for ads in colles_ads:
            delt_infos = self.find_belt_name(ads['delt_name'])
            ads['pt'] = self.check_interests_behaviors(ads['pt'])

            for delt_info in delt_infos:
                # bbt = delt_info['name'].split('_')[0]
                for creative_media in self.urls:
                    self.pts.append({'hash': str(uuid.uuid4()),
                                     'pt': copy.deepcopy(self.modify_pt(delt_info['name'], delt_info['platform'],
                                                                        delt_info['country'], ads['pt'],
                                                                        creative_media),
                                                         ),
                                     'algo': 'random',
                                     'status': 'available',
                                     'country': delt_info['country'],
                                     'platform': delt_info['platform'],
                                     'delt_name': delt_info['name'],
                                     'created_at': datetime.datetime.now(tz).strftime('%Y-%m-%dT%H:%M:%SZ')
                                     })

    def check_interests_behaviors(self, pt):
        if pt.get('adset_spec') and pt['adset_spec'].get('targeting'):
            if pt['adset_spec']['targeting'].get('interests'):
                interests = pt['adset_spec']['targeting']['interests']
                tmp_interests = list()
                if isinstance(interests, list):
                    for interest in interests:
                        if isinstance(interest, dict) and self.interests.get(int(interest['id'])):
                            tmp_interests.append({'id': interest['id'], 'name': interest['name']})
                elif isinstance(interests, dict):
                    for values in interests.values():
                        if isinstance(values, dict) and self.interests.get(int(values['id'])):
                            tmp_interests.append({'id': values['id'], 'name': values['name']})
                pt['adset_spec']['targeting']['interests'] = copy.deepcopy(tmp_interests)
            if pt['adset_spec']['targeting'].get('behaviors'):
                behaviors = pt['adset_spec']['targeting']['behaviors']
                tmp_behaviors = list()
                if isinstance(behaviors, list):
                    for behavior in behaviors:
                        if isinstance(behavior, dict) and self.behaviors.get(behavior['id']):
                            tmp_behaviors.append({'id': behavior['id'], 'name': behavior['name']})
                elif isinstance(behaviors, dict):
                    for values in behaviors.values():
                        if isinstance(values, dict) and self.behaviors.get(values['id']):
                            tmp_behaviors.append({'id': values['id'], 'name': values['name']})
                pt['adset_spec']['targeting']['behaviors'] = copy.deepcopy(tmp_behaviors)
        return pt

    def find_reports(self):
        colles_report = self.db.get_collection(self.db_report_name).find({'cohort_date': {'$lt': '2018-09-01'},
                                                                          'cost': {'$gt': 1}, 'install': {'$gte': 3},
                                                                          'revenue_day1': {'$gt': 0}}, {'_id': 0,
                                                                                                        'ad_id': 1})
        ad_ids = [rp['ad_id'] for rp in colles_report]
        logger.info('the size of report\'s ads is %d' % (len(ad_ids)))
        self.find_ads(list(set(ad_ids)))

    def insert_mongodb(self):
        index = 0
        for pt in self.pts:
            try:
                self.db.baits.insert(pt)
                index += 1
            except:
                pass
        logger.info('the size of insert into mongodb is %d' % (index))

    def main(self):
        t = time.time()
        logger.info('start...')
        self.select_mysql()
        self.mongodb_conn()
        self.find_reports()
        logger.info('insert into mongodb...')
        self.insert_mongodb()
        logger.info('inserted...')
        self.close()
        logger.info('it cost %d seconds...' % (time.time()-t))


def tmain():
    try:
        uba = UpdateBeforeAds()
        uba.main()
        return "OK"
    except:
        return "Fail"


if __name__ == '__main__':
    tmain()


