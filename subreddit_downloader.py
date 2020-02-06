import json
from sys import argv, exit
from time import sleep, time

import pandas as pd
import requests

if __name__=="__main__":
    try:
        subreddit_name = str(argv[1]).strip()
    except:
        print('error: please input the name of subreddit as a first argument. ex: python subreddit_downloader.py [subreddit name]')
        exit()
    
    headers = {'Content-Type': 'application/json; charset=utf-8', 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0'}
    url_subreddit = f"https://www.reddit.com/r/{subreddit_name}/new/.json"

    params = {
        'after': 'null',
        'limit': 100
        }

    # scraping posts 
    """
    scraping columns: 
        data.children.~~~.kind
        data.children.~~~.data.
            title
            author
            selftext
            link_flair_text
            ups : number of up votes
            created_utc
            id : post_id
            num_comments
            url : attatched url
            permalink
    """
    print(f"Scraping posts in subreddit '{subreddit_name}'")
    df_posts = pd.DataFrame([], columns=['type_prefix', 'id', 'title',  'author', 'selftext', 'link_flair_text', 'up_votes', 'created_utc', 'num_comments', 'url', 'permalink'])
    while True:
        # web request
        response = requests.get(url=url_subreddit, params=params, headers=headers)
        if response.status_code != 200:
            print(f'error code :{response.status_code}')
            sleep(3) # if there is an error, wait 3 second and try again
            continue
        
        # for next page
        str_json = response.json()
        if str_json['data']['after'] == None:
            break # while loop end condition
        else:
            params['after'] = str_json['data']['after']

        # get contents
        for post in str_json['data']['children']:
            df_posts.loc[len(df_posts)] = [
                post['kind'], post['data']['id'], post['data']['title'], 
                post['data']['author'], post['data']['selftext'], 
                post['data']['link_flair_text'], post['data']['ups'], 
                post['data']['created_utc'], post['data']['num_comments'], 
                post['data']['url'], "https://www.reddit.com"+post['data']['permalink']
                ]
        sleep(1) # to prevent the lost connection


    # scraping comments 
    """
    scraping columns: 
        1.data.children.~~~.kind
        1.data.children.~~~.data.
            author
            body
            ups : number of up votes
            created_utc
            id : comment_id
            parent_id
            permalink
    """

    print(f"Scraping comments in subreddit '{subreddit_name}'")
    df_comments = pd.DataFrame([], columns=['type_prefix', 'id',  'parent_id', 'author', 'body', 'up_votes', 'created_utc', 'permalink'])
    for permalink_post in list(df_posts.permalink):
        # web request
        url_post = f"{permalink_post}/.json"
        response = requests.get(url=url_post, headers=headers)
        if response.status_code != 200:
            print(f'error code :{response.status_code}')
            sleep(3) # if there is an error, wait 3 second and try again
            continue
        
        # get contents
        str_json = response.json()
        for comment in str_json[1]['data']['children']:
            try:
                df_comments.loc[len(df_comments)] = [
                    comment['kind'], comment['data']['id'], comment['data']['parent_id'], comment['data']['author'], comment['data']['body'],
                    comment['data']['ups'], comment['data']['created_utc'], "https://www.reddit.com"+comment['data']['permalink']
                    ]
            except:
                continue
            if comment['data']['replies'] != '':
                replies = comment['data']['replies']['data']['children']
                # scraping multi-depth replies 
                for reply in replies:
                    try:
                        df_comments.loc[len(df_comments)] = [
                            reply['kind'], reply['data']['id'], reply['data']['parent_id'], reply['data']['author'], reply['data']['body'],
                            reply['data']['ups'], reply['data']['created_utc'], "https://www.reddit.com"+reply['data']['permalink']
                            ]
                    except:
                        continue
                    if reply['data']['replies'] != '':
                        replies = replies + reply['data']['replies']['data']['children']
        sleep(1) # to prevent the lost connection


    # saving to excel files
    time_current = round(time())
    path_save = f"subreddit_{subreddit_name}_{time_current}.xlsx"
    
    with pd.ExcelWriter(path_save) as writer:  
        df_posts.to_excel(writer, sheet_name='posts', index=False)
        df_comments.to_excel(writer, sheet_name='comments', index=False)
    print(f"File was saved with the name of '{path_save}'")