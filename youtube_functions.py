import googleapiclient.discovery
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
import time
from datetime import date
from datetime import datetime
import goslate
import sentiment_analysis_functions as saf
# from textblob import TextBlob - another NLP option
# https://dev.to/kalebu/how-to-do-language-translation-in-python-1ic6

def youtube_connect(api_key):
    connection = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)
    return connection
    
def get_video_stats(video_id, connection):
    if type(video_id) == str and len(str(video_id)) == 11:
        request = connection.videos().list(part='statistics', id=video_id)
        result = request.execute()
        statistics = json_normalize(result['items'][0]['statistics'])
        #statistics['video_id'] = video_id
    else:
        statistics = pd.DataFrame(columns=['entry'])
    return statistics

def get_channel_stats(channel_id, connection):
    if type(channel_id) == str and len(str(channel_id)) == 24:
        request = connection.channels().list(part='statistics', id=channel_id)
        result = request.execute()
        statistics = json_normalize(result['items'][0]['statistics'])
        #statistics['channel_id'] = channel_id
    else:
        statistics = pd.DataFrame(columns=['entry'])
    return statistics

def get_video_comments(video_id, connection):
    if type(video_id) == str and len(str(video_id)) == 11:
        song_comments = pd.DataFrame(columns=['date', 'text', 'likes'])
        next_page = True
        nextPageToken = None
        while next_page == True:
            request = connection.commentThreads().list(part='snippet', videoId=video_id, maxResults=100, pageToken=nextPageToken)
            result = request.execute()
            for comment in result['items']:
                col_likes = comment['snippet']['topLevelComment']['snippet']['likeCount']
                if col_likes >= 0:
                    col_date = comment['snippet']['topLevelComment']['snippet']['publishedAt']
                    col_date = datetime.strptime( col_date[:10], '%Y-%m-%d').date()
                    col_text = comment['snippet']['topLevelComment']['snippet']['textDisplay']
                    comment_details = pd.DataFrame({"video_id": [video_id], "date": [col_date], "text": [col_text], "likes": [col_likes]})
                    song_comments = song_comments.append(comment_details, sort=True)
            if 'nextPageToken' in result:
                nextPageToken = result['nextPageToken']
            else:
                next_page = False
    else:
        song_comments = pd.DataFrame(columns=['entry'])
    return song_comments



def get_songs_stats(year, api_ids, connection):
    countries = api_ids[api_ids['year']==year]['entry'].reset_index(drop=True)
    video_ids = api_ids[api_ids['year']==year]['youtube_song_video_id'].reset_index(drop=True)
    all_song_stats = pd.DataFrame(columns=['date', 'entry', 'viewCount', 'likeCount', 'dislikeCount', 'commentCount', 'favoriteCount'])
    for video in range(len(video_ids)):
        video_stats = get_video_stats(video_ids[video], connection)
        video_stats['entry'] = countries[video]
        video_stats['date'] = date.today().strftime("%d%m%Y")
        all_song_stats = all_song_stats.append(video_stats, sort=True)
    all_song_stats = all_song_stats[['date', 'entry', 'viewCount', 'likeCount', 'dislikeCount', 'commentCount', 'favoriteCount']].rename(columns={'viewCount': 'youtube_song_views', 'likeCount': 'youtube_song_likes', 'dislikeCount': 'youtube_song_dislikes', 'commentCount': 'youtube_song_comments', 'favoriteCount': 'youtube_song_favorites'})
    all_song_stats['youtube_song_views'] = all_song_stats['youtube_song_views'].astype(float)
    all_song_stats['youtube_song_likes'] = all_song_stats['youtube_song_likes'].astype(float)
    all_song_stats['youtube_song_dislikes'] = all_song_stats['youtube_song_dislikes'].astype(float)
    all_song_stats['youtube_song_comments'] = all_song_stats['youtube_song_comments'].astype(float)
    all_song_stats['youtube_song_favorites'] = all_song_stats['youtube_song_favorites'].astype(float)
    return all_song_stats
    
def get_artists_stats(year, api_ids, connection):
    countries = api_ids[api_ids['year']==year]['entry'].reset_index(drop=True)
    channel_ids = api_ids[api_ids['year']==year]['youtube_artist_channel_id'].reset_index(drop=True)
    all_artist_stats = pd.DataFrame(columns=['date', 'entry', 'viewCount', 'commentCount', 'subscriberCount', 'hiddenSubscriberCount', 'videoCount'])
    for channel in range(len(channel_ids)):
        channel_stats = get_channel_stats(channel_ids[channel], connection)
        channel_stats['entry'] = countries[channel]
        channel_stats['date'] = date.today().strftime("%d%m%Y")
        all_artist_stats = all_artist_stats.append(channel_stats, sort=True)
    all_artist_stats = all_artist_stats[['date', 'entry', 'viewCount', 'commentCount', 'subscriberCount', 'hiddenSubscriberCount', 'videoCount']]
    return all_artist_stats
    
def get_songs_comments(year, api_ids, connection):
    #gs = goslate.Goslate()
    countries = api_ids[api_ids['year']==year]['entry'].reset_index(drop=True)
    video_ids = api_ids[api_ids['year']==year]['youtube_song_video_id'].reset_index(drop=True)
    all_song_comments = pd.DataFrame(columns=['entry', 'date', 'text', 'likes'])
    for video in range(len(video_ids)):
        song_comments = get_video_comments(video_ids[video], connection)
        song_comments['entry'] = countries[video]
        #song_comments['text_en'] = [gs.translate(i, 'en') for i in song_comments['text']]
        #time.sleep(2)
        all_song_comments = all_song_comments.append(song_comments, sort=True)
    all_song_comments = saf.sentiment_analysis(all_song_comments)
    return all_song_comments

def get_comment_stats(comments):
    sentiment_metrics = saf.get_sentiment_metrics(comments)
    comment_stats = sentiment_metrics.groupby('entry').agg(comments_total_love=pd.NamedAgg(column='love', aggfunc=np.sum),
                                                           comments_proportional_love=pd.NamedAgg(column='love', aggfunc=np.mean),
                                                           comments_total_positivity=pd.NamedAgg(column='positive_sentiment', aggfunc=np.sum),
                                                           comments_average_positivity=pd.NamedAgg(column='positive_sentiment', aggfunc=np.mean),
                                                           comments_total_douze_points=pd.NamedAgg(column='douze_points', aggfunc=np.sum),
                                                           comments_weighted_douze_points=pd.NamedAgg(column='weighted_douze_points', aggfunc=np.sum),
                                                           comments_proportional_douze_points=pd.NamedAgg(column='douze_points', aggfunc=np.mean),
                                                           comments_total_weighted_sentiment=pd.NamedAgg(column='weighted_sentiment', aggfunc=np.sum),
                                                           )
    comment_stats = comment_stats.reset_index()
    comment_stats['date'] = date.today().strftime("%d%m%Y")
    comment_stats['comments_total_love'] = comment_stats['comments_total_love'].astype(float)
    comment_stats['comments_proportional_love'] = comment_stats['comments_proportional_love'].astype(float)
    comment_stats['comments_total_positivity'] = comment_stats['comments_total_positivity'].astype(float)
    comment_stats['comments_average_positivity'] = comment_stats['comments_average_positivity'].astype(float)
    comment_stats['comments_total_douze_points'] = comment_stats['comments_total_douze_points'].astype(float)
    comment_stats['comments_weighted_douze_points'] = comment_stats['comments_weighted_douze_points'].astype(float)
    comment_stats['comments_proportional_douze_points'] = comment_stats['comments_proportional_douze_points'].astype(float)
    comment_stats['comments_total_weighted_sentiment'] = comment_stats['comments_total_weighted_sentiment'].astype(float)
    return comment_stats
