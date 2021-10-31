import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pickle
from time import sleep, time
from bs4 import BeautifulSoup
import pandas as pd
from collections import deque
import re
from datetime import datetime


class ModifiableCycle(object):
    def __init__(self, items=()):
        self.deque = deque(items)

    def __iter__(self):
        return self
    def __next__(self):
        if not self.deque:
            raise StopIteration
        item = self.deque.popleft()
        self.deque.append(item)
        return item
    next = __next__
    def delete_next(self):
        self.deque.popleft()
    def delete_prev(self):
        self.deque.pop()


accounts = pd.read_csv(
    "accounts.txt", sep=':', header=None)
accounts.columns = ['acc', 'password', 'mail', 'mail_pass']

proxies = pd.read_csv(
    "proxies.txt", sep=':', header=None)
proxies.columns = ['ip', 'port', 'user', 'password']

user_agents = pd.read_table('user_agents.txt', sep='\t')

broken_accounts = []
small_bloggers_dict = {}

user_agents_row_list = ModifiableCycle(list(range(user_agents.shape[0])))
proxy_row_list = ModifiableCycle(list(range(proxies.shape[0])))
account_row_list = ModifiableCycle(list(range(accounts.shape[0])))


def login():
    link = 'https://www.instagram.com/accounts/login/'
    login_url = 'https://www.instagram.com/accounts/login/ajax/'
    time = int(datetime.now().timestamp())
    status = ''
    while status != 'ok':
        ua_row = next(user_agents_row_list)
        proxy_row = next(proxy_row_list)
        account_row = next(account_row_list)
        try:
            payload = {
                'username': f'{accounts.iloc[account_row].acc}',
                'enc_password': f'#PWD_INSTAGRAM_BROWSER:0:{time}:{accounts.iloc[account_row].password}',
                'queryParams': {},
                'optIntoOneTap': 'false'
            }

            s = requests.Session()
            s.proxies.update({'http': f'http://{proxies.iloc[proxy_row].user}:{proxies.iloc[proxy_row].password}@{proxies.iloc[proxy_row].ip}:{proxies.iloc[proxy_row].port}'})
            s.headers.update({'User-Agent': f'{user_agents.iloc[ua_row]["User agent"]}'})
            r = s.get(link, headers={
                        'User-Agent': f'{user_agents.iloc[ua_row]["User agent"]}'})
            csrf = re.findall(r"csrf_token\":\"(.*?)\"", r.text)[0]
            r = s.post(login_url, data=payload, headers={
                "user-agent": f"{user_agents.iloc[ua_row]['User agent']}",
                "x-requested-with": "XMLHttpRequest",
                "referer": "https://www.instagram.com/accounts/login/",
                "x-csrftoken": csrf
            })
            try:
                if r.json()['message'] == 'checkpoint_required' or r.json()['lock']:
                    broken_accounts.append(accounts.iloc[account_row].acc)
                    account_row_list.delete_prev()
            except:
                pass
            status = r.json()['status']
            sleep(4)
            print(r.status_code)
            print(r.text)
            print('account:\n',accounts.iloc[account_row])
            print('account:\n',proxies.iloc[proxy_row])
        except:
            pass
    return s


def get_user_info(session, user_id):
    info = session.get(
        f'https://i.instagram.com/api/v1/users/{str(user_id)}/info/'
    )
    return info


def get_followers(session, user_info):
    if user_info.json()['user']['is_private']:
        return None
    else:
        user_id = str(user_info.json()['user']['pk'])
        follower_count = user_info.json()['user']['follower_count']
        followers = session.get(
            f'https://i.instagram.com/api/v1/friendships/{user_id}/followers/?count={follower_count}'
        )
        return followers


def get_media(session, user_info):
    if user_info != None and user_info.json()['user']['is_private']==False:
        user_id = str(user_info.json()['user']['pk'])
        posts_count = user_info.json()['user']['media_count']
        my_query = {
            'query_hash': '8c2a529969ee035a5063f2fc8602a0fd',
            'variables': f'{{"id": "{user_id}", "first": {posts_count}, "after": ""}}'
        }
        graphql = session.get(
            'https://www.instagram.com/graphql/query/', params=my_query)
        return graphql


df_info_main = pd.DataFrame()
df_followers_main = pd.DataFrame()
df_media_main = pd.DataFrame()

with open('user_ids.pkl', 'rb') as file:
    user_ids = pickle.load(file)
    file.close()


def making_table(session, user_id):
    status_code_info = 0
    status_code_followers = 0
    status_code_media = 0
    try_count = 0
    global s
    while True:
        if try_count<4:
            try_count+=1
            pass
        else:
            return None, None, None, []
        try:
            user_info = get_user_info(session, user_id)
            try:
                status_code_info = user_info.status_code
                if status_code_info >= 400:
                    raise
            except:
                return None, None, None, []
            if status_code_info == 200:
                break
            else:
                raise
        except:
            if user_info.json()['lock']:
                broken_accounts.append(accounts.iloc[account_row].acc)
                account_row_list.delete_prev()
            print('!!! USER INFO CHANGED THE PROXY !!!')
            print(user_info)
            s = login()
    sleep(3)
    
    if user_info.json()['user']['is_private']:
        user_info = user_info.json()['user']
        df_info = pd.json_normalize(user_info)
        return df_info, None, None, []
    else:
        pass

    while True:
        try:
            user_followers = get_followers(session, user_info)
            status_code_followers = user_followers.status_code
            if status_code_followers == 200:
                break
            else:
                raise
        except:
            print('!!! USER FOLLOWERS CHANGED THE PROXY !!!')
            print(user_info['username'])
            s = login()
    sleep(3)
    while True:
        try:
            user_media = get_media(session, user_info)
            status_code_media = user_media.status_code
            if status_code_followers == 200:
                break
            else:
                raise
        except:
            print('!!! USER MEDIA CHANGED THE PROXY !!!')
            s = login()
    timeout = time() + 20
    while time() < timeout:
        user_info = user_info.json()['user']
        print('parsing: ', user_info['username'])
        df_info = pd.json_normalize(user_info)
        user_followers = user_followers.json()['users']
        print('followers collected')
        user_media = user_media.json()['data']['user']['edge_owner_to_timeline_media']['edges']
        print('media_collected')
        ids_list = []
        for i in user_followers:
            ids_list.append(i['pk'])
        df_followers = pd.json_normalize(user_followers)
        df_followers['source_user'] = user_info['username']
        df_media = pd.json_normalize(user_media)
        df_media['source_user'] = user_info['username']
        print('Success!!')
        return df_info, df_followers, df_media, ids_list
    s = login()
    return None, None, None, []


s = login()
for id in user_ids:
    try:
        df_info, df_followers, df_media, ids_list = making_table(s, id)
    except:
        continue
    df_info_main = df_info_main.append(df_info)
    df_followers_main = df_followers_main.append(df_followers)
    df_media_main = df_media_main.append(df_media)
    user_ids += ids_list
    print('writing files')
    with open('df_info_main.pkl', 'wb') as file:
        pickle.dump(df_info_main, file)
        file.close()
    with open(r'df_followers_main.pkl', 'wb') as file:
        pickle.dump(df_followers_main, file)
        file.close()
    
    with open('df_media_main.pkl', 'wb') as file:
        pickle.dump(df_media_main, file)
        file.close()
    with open('user_ids.pkl', 'wb') as file:
        pickle.dump(user_ids, file)
        file.close()
    print('finished writing files')
    sleep(10)
