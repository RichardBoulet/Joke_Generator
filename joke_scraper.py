#!/usr/bin/env python


# ## Scraper for r/jokes subreddit that will be used to train NLP model

from collections import defaultdict
import datetime
from datetime import datetime
import pandas as pd
import praw
import time

# Test request of jokes subreddit, hot posts, limited
import r_auth

# Set reddit api reqs
reddit = praw.Reddit(client_id = r_auth.client_id,
                     client_secret = r_auth.client_secret,
                     user_agent = r_auth.user_agent,
                     username = r_auth.username,
                     password = r_auth.password)



# Set parameters for Jokes subreddit query
submissions = reddit.subreddit('Jokes').hot(limit = 100)


# Set timeframe for joke scraping to one month
TODAY = datetime.today()

timestamp = time.mktime(TODAY.timetuple())
seconds_one_month = 2_628_288 * 100
one_month_timeframe = timestamp - seconds_one_month


# Create default dictionary, and then set the keys in the for loop for each required field
joke_dict = defaultdict(list)

for joke in submissions:
    if joke.score > 200:
        joke_dict['id'].append(joke.id)
        joke_dict['date'].append(datetime.utcfromtimestamp(joke.created).strftime('%Y-%m-%d'))
        joke_dict['title'].append(joke.title)
        joke_dict['body'].append((joke.selftext).replace('\\', ''))
        joke_dict['score'].append(joke.score)

# Convert the ListingGenerator from Praw to Datafame for feeding to Dynamo
jokes_scrape = pd.DataFrame(joke_dict)



# DYNAMO SECTION

# Create records of jokes in AWS
import boto3

# Set AWS resource type to dynamo
dynamodb = boto3.resource('dynamodb')

# Set AWS dynamo table by setting name of table in dynamo dash
table = dynamodb.Table('jokes_table')


# Iterate through dataframe (iterrows may be too slow for larger datasets, but fine for 100-at-a-time records)
with table.batch_writer() as batch:
    for index, row in jokes_scrape.iterrows():
        batch.put_item(
            Item = {
                'id': row['id'],
                'date': row['date'],
                'title': row['title'],
                'body': row['body'],
                'score': row['score']
    }
)

