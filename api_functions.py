import csv
import youtube_functions
import exchange_functions
import spotify_functions
import twitter_functions

def youtube_api_data(youtube_api_key, contest, api_ids, data_path, today):
    youtube_connection = youtube_functions.youtube_connect(youtube_api_key)
    youtube_song_stats = youtube_functions.get_songs_stats(contest, api_ids, youtube_connection)
    youtube_song_stats.to_csv(str(data_path+'\\'+contest+'\\youtube_song\\youtube_song_'+today+'.csv'), index=False)
    youtube_artist_stats = youtube_functions.get_artists_stats(contest, api_ids, youtube_connection)
    youtube_artist_stats.to_csv(str(data_path+'\\'+contest+'\\youtube_artist\\youtube_artist_'+today+'.csv'), index=False)
    return youtube_song_stats, youtube_artist_stats#, youtube_comment_stats

def youtube_api_comments(youtube_api_key, contest, api_ids, data_path, today):
    youtube_connection = youtube_functions.youtube_connect(youtube_api_key)
    youtube_comments = youtube_functions.get_songs_comments(contest, api_ids, youtube_connection)
    youtube_comments.to_csv(str(data_path+'\\'+contest+'\\youtube_comments.csv'), index=False, quoting=csv.QUOTE_ALL)
    youtube_comment_stats = youtube_functions.get_comment_stats(youtube_comments)
    youtube_comment_stats.to_csv(str(data_path+'\\'+contest+'\\youtube_comment\\youtube_comment_'+today+'.csv'), index=False)
    return youtube_comment_stats


def exchange_api_data(exchange_api_key, contest, market_ids, data_path, today):
    exchange_odds = exchange_functions.get_exchange_odds(contest, market_ids, exchange_api_key)
    exchange_odds.to_csv(str(data_path+'\\'+contest+'\\exchange_odds\\exchange_odds_'+today+'.csv'), index=False)
    return exchange_odds

def spotify_api_data(spotify_client_id, spotify_client_secret, contest, api_ids, data_path, today):
    spotify_connection = spotify_functions.spotify_connect(spotify_client_id, spotify_client_secret)
    spotify_song_stats, spotify_artist_stats = spotify_functions.get_songs_and_artists_stats(contest, api_ids, spotify_connection)
    spotify_song_stats.to_csv(str(data_path+'\\'+contest+'\\spotify_song\\spotify_song_'+today+'.csv'), index=False)
    spotify_artist_stats.to_csv(str(data_path+'\\'+contest+'\\spotify_artist\\spotify_artist_'+today+'.csv'), index=False)
    return spotify_song_stats, spotify_artist_stats

def twitter_api_data(twitter_consumer_key, twitter_consumer_secret, twitter_access_key, twitter_access_secret, contest, api_ids, data_path, today):
    twitter_connection = twitter_functions.twitter_connect(twitter_consumer_key, twitter_consumer_secret, twitter_access_key, twitter_access_secret)
    twitter_posts = twitter_functions.get_all_tweets(contest, api_ids, twitter_connection)
    twitter_posts.to_csv(str(data_path+'\\'+contest+'\\twitter_song\\twitter_song_'+today+'.csv'), index=False, quoting=csv.QUOTE_ALL)
    twitter_stats = get_tweet_stats(twitter_posts)
    twitter_stats.to_csv(str(data_path + '\\' + contest + '\\twitter_stats\\twitter_stats_' + today + '.csv'), index=False)
    # artist......
    return twitter_stats

## Facebook
# facebook artist stats

## Instagram
# instagram artist stats

## Google Trends
# song search stats
# artist search stats