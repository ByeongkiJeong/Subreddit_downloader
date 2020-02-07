# Subreddit downloader 
- This python code download a target subreddit of reddit
- Do not need reddit API key
- subreddit_downloader.py do not use any API, but it can download only unarchived submissions(live submissions)
- subreddit_downloader_pushshift_api.py uses Pushshift API, and it download 5 years of the inputed subreddit from now (you can change it in the code by changing '5y' to '1m', '60d', etc.).
- The data will be saved as a xlsx format

# How to use
```cmd
python subreddit_downloader.py [subreddit name]
```
or  
```cmd
python subreddit_downloader_pushshift_api.py [subreddit name]
```

# Requirement
- Python > 3.7 (This code uses f-string to string formatting)
- Pandas

# Others
- This code may has errors because it still work-in-progress
- subreddit_downloader.py was inpired from 'https://unnikked.ga/scraping-a-subreddit-with-a-one-liner-6322f74134df'
- subreddit_downloader_pushshift_api.py also was inspired from 'https://www.reddit.com/r/pushshift/comments/89pxra/pushshift_api_with_large_amounts_of_data/'
