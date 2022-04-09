import spotipy
import pandas as pd
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import date


def spotify_connect(client_id, client_secret):
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    connection = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    return connection

def get_song_and_artist_stats(date, song_uri, entry, connection):
    if type(song_uri) == str and len(str(song_uri)) == 36:
        track_details = connection.track(song_uri)
        song_stats = pd.DataFrame(data={'date': date, 'entry': entry, 'spotify_song_popularity': [float(track_details['popularity'])]})
        artist_uri = track_details['album']['artists'][0]['uri']
        artist_details = connection.artist(artist_uri)
        artist_stats = pd.DataFrame(data={'date': date, 'entry': entry, 'spotify_artist_popularity': [float(artist_details['popularity'])], 'spotify_artist_followers': [float(artist_details['followers']['total'])]})
    else:
        song_stats = pd.DataFrame(columns=['date', 'entry'])
        artist_stats = pd.DataFrame(columns=['date', 'entry'])
    return song_stats, artist_stats

def get_songs_and_artists_stats(year, api_ids, connection):
    countries = api_ids[api_ids['year']==year]['entry'].reset_index(drop=True)
    song_uris = api_ids[api_ids['year']==year]['spotify_song_uri_id'].reset_index(drop=True)
    all_song_stats = pd.DataFrame(columns=['date', 'entry', 'spotify_song_popularity'])
    all_artist_stats = pd.DataFrame(columns=['date', 'entry', 'spotify_artist_popularity', 'spotify_artist_followers'])
    for song in range(len(countries)):
        song_stats, artist_stats = get_song_and_artist_stats(date.today().strftime("%d%m%Y"), song_uris[song], countries[song], connection)
        all_song_stats = all_song_stats.append(song_stats, sort=True)
        all_artist_stats = all_artist_stats.append(artist_stats, sort=True)
    return all_song_stats, all_artist_stats
