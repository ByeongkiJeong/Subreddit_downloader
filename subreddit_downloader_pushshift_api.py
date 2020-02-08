from sys import argv
from time import sleep, time

import pandas as pd
import requests


def get_pushshift(subreddit_name=None, before=None, after=None, searchType='submission'):
    suffix=''
    if (before is not None):
        suffix += f'&before={before}'
    if (after is not None):
        suffix += f'&after={after}'
    if (subreddit_name is not None):
        suffix += f'&subreddit={subreddit_name}'

    url = f'https://api.pushshift.io/reddit/search/{searchType}?sort=desc&size=1500{suffix}'
    #print('loading '+url)
    r = requests.get(url)
    data = r.json()
    if len(data['data']) > 0:
        prev_end_date = data['data'][-1]['created_utc']
    else:
        prev_end_date = None
    return (data, prev_end_date)

if __name__=="__main__":
    try:
        subreddit_name = str(argv[1]).strip()
    except:
        print('error: please input the name of subreddit as a first argument. ex: python subreddit_downloader.py [subreddit name]')
        exit()

    # scraping posts 
    """
    scraping columns: 
        data.
            id, title, author, selftext, link_flair_text, created_utc, num_comments, url(attatched url), permalink
    """
    print(f"Scraping posts in subreddit '{subreddit_name}'")
    df_posts = pd.DataFrame([], columns=['id', 'title',  'author', 'selftext', 'link_flair_text', 'created_utc', 'num_comments', 'url', 'permalink'])

    prev_end_date = 9999999999
    while prev_end_date is not None:
        submissions, prev_end_date = get_pushshift(subreddit_name=subreddit_name, before=prev_end_date-1, after='5y', searchType='submission')
        if prev_end_date is not None:
            for post in submissions['data']:
                df_posts.loc[len(df_posts)] = [
                    post.get('id',''), post.get('title',''), post.get('author',''), post.get('selftext',''), post.get('link_flair_text',''), 
                    post.get('created_utc',''), post.get('num_comments',''), post.get('url',''), post.get('permalink','')
                    ]
        sleep(1)


    # scraping comments 
    """
    scraping columns: 
        data.
            id, parent_id, author, body, created_utc, permalink
    """

    print(f"Scraping comments in subreddit '{subreddit_name}'")
    df_comments = pd.DataFrame([], columns=['id', 'parent_id', 'author', 'body', 'created_utc', 'permalink'])

    prev_end_date = 9999999999
    while prev_end_date is not None:
        comments, prev_end_date = get_pushshift(subreddit_name=subreddit_name, before=prev_end_date-1, after='5y', searchType='comment')
        if prev_end_date is not None:
            for comment in comments['data']:
                df_comments.loc[len(df_comments)] = [
                    comment['id'], comment['parent_id'], comment['author'], comment['body'], comment['created_utc'], comment['permalink']
                    ]
        sleep(1)


    # saving to excel files
    time_current = round(time())
    path_save = f"subreddit_{subreddit_name}_{time_current}.xlsx"

    with pd.ExcelWriter(path_save) as writer:  
        df_posts.to_excel(writer, sheet_name='posts', index=False)
        df_comments.to_excel(writer, sheet_name='comments', index=False)
    print(f"File was saved with the name of '{path_save}'")
