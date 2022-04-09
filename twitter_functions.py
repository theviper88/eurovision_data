import tweepy
import pandas as pd
import datetime

def twitter_connect(consumer_key, consumer_secret, access_key, access_secret):
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    connection = tweepy.API(auth)
    return connection

def get_tweets(search_string, start_date, end_date, connection):
    song_tweets = pd.DataFrame(columns=['tweet_id', 'date', 'text', 'favourites', 'retweets'])
    last_tweet = None
    tweet_date = datetime.date.today()
    no_calls = 0
    while no_calls<1:#tweet_date > start_date:
        for tweet in tweepy.Cursor(connection.search, search_string, rpp = 100).items(100):
            tweet_date = tweet.created_at.date()
            if no_calls<1: #tweet_date > start_date and tweet_date < end_date:
                #tweet_details = [tweet.id_str, tweet.created_at, tweet.text.encode("utf-8"), tweet.retweet_count, tweet.favorite_count]
                tweet_details = pd.DataFrame({"tweet_id": [tweet.id_str], "date": [tweet.created_at], "text": [tweet.text.encode("utf-8")], "favourites": [tweet.favorite_count], "retweets": [tweet.retweet_count]})
                song_tweets = song_tweets.append(tweet_details, sort=True)
                last_tweet = tweet.id_str
        no_calls = no_calls + 1
    return song_tweets

def get_all_tweets(year, api_ids, connection):
    countries = api_ids[api_ids['year'] == int(year)]['entry']
    twitter_keywords = api_ids[api_ids['year'] == int(year)]['twitter_keywords']
    eurovision_tweets = get_tweets('eurovision', datetime.date.today()-datetime.timedelta(days=0), datetime.date.today(), connection)
    esc_tweets = get_tweets('esc', datetime.date.today()-datetime.timedelta(days=0), datetime.date.today(), connection)
    eurovision_tweets = pd.concat([eurovision_tweets, esc_tweets]).drop_duplicates().reset_index(drop=True)
    return eurovision_tweets

def get_tweet_stats(posts):
    sentiment_metrics = saf.get_sentiment_metrics(posts)
    # need to get country #
    post_stats = sentiment_metrics.groupby('date', 'entry').weighted_love.agg('sum')
    #comment_stats.columns = comment_stats.columns.droplevel(0)
    return post_stats