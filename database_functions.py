# database functions
from typing import List
import pyodbc
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from collections import OrderedDict
import time

def read_ids(google_sheets_key, events_names):
    scope: List[str] = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(google_sheets_key, scope)
    gsheets_api = gspread.authorize(credentials)

    all_api_ids = OrderedDict()
    api_id_sheet = gsheets_api.open('api_ids')
    for events_name in events_names:
        sheet_tab = api_id_sheet.worksheet(events_name)
        list_of_lists = sheet_tab.get_all_values()
        api_ids = pd.DataFrame.from_records(list_of_lists)
        api_ids.columns = api_ids.iloc[0]
        api_ids.drop(api_ids.index[0], inplace=True)
        all_api_ids.update({events_name: api_ids})

    all_market_ids = OrderedDict()
    api_id_sheet = gsheets_api.open('exchange_ids')
    for events_name in events_names:
        sheet_tab = api_id_sheet.worksheet(events_name)
        list_of_lists = sheet_tab.get_all_values()
        market_ids = pd.DataFrame.from_records(list_of_lists)
        market_ids.columns = market_ids.iloc[0]
        market_ids.drop(market_ids.index[0], inplace=True)
        all_market_ids.update({events_name: market_ids})

    return all_api_ids, all_market_ids



def write_data(full_data_set, google_sheets_key, contest, event):
    ## connect to Google Sheets ##
    scope: List[str] = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(google_sheets_key, scope)
    gsheets_api = gspread.authorize(credentials)
    sheet_tab = gsheets_api.open('api_data_current').worksheet(event)
    historics = sheet_tab.col_values(2)
    for row in range(len(full_data_set)):
        print(row)
        print(list(full_data_set.iloc[row]))
        sheet_tab.insert_row(list(full_data_set.iloc[row]), len(historics)+1+row)
        time.sleep(5)
    print('SUCCESS: full_data_set uploaded - '+ event)



def data_trends_insert(data_frame):

    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=localhost\SQLEXPRESS;'
                          'Database=master;'
                          'Trusted_Connection=yes;')
    cursor = conn.cursor()

    query = "DROP TABLE IF EXISTS data.song_trends; \
             CREATE TABLE data.song_trends('date' DATE, 'country' VARCHAR(50), 'exchange_odds' decimal, \
                                           'youtube_song_views' INT, 'youtube_song_likes' INT, 'youtube_song_comments' INT, \
                                           'youtube_song_dislikes' INT, 'youtube_song_favourites' INT, 'spotify_song_popularity' INT, \
                                           'spotify_artist_popularity' INT, 'spotify_artist_followers' INT); \
            Insert ? into data.song_trends;"
    
    cursor.execute(query, data_frame)

    cursor.close()
