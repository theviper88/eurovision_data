import pandas as pd
from datetime import date
import time
import api_functions
import qa_functions_
import database_functions

# set parameters #
contest = '2022'
events = [{'event_name': 'final', 'event_date': date(2022, 5, 14)},
          {'event_name': 'semi_final_1', 'event_date': date(2022, 5, 10)},
          {'event_name': 'semi_final_2', 'event_date': date(2022, 5, 12)},
          {'event_name': 'melfest', 'event_date': date(2022, 3, 12)},
          {'event_name': 'sanremo', 'event_date': date(2022, 2, 5)},
          {'event_name': 'mgp(n)', 'event_date': date(2022, 2, 19)},
          {'event_name': 'umk', 'event_date': date(2022, 2, 26)},
          {'event_name': 'eesti_laul', 'event_date': date(2022, 2, 12)},
          {'event_name': 'portugal', 'event_date': date(2022, 3, 12)},
          {'event_name': 'mgp(d)', 'event_date': date(2022, 3, 5)},
          {'event_name': 'dora', 'event_date': date(2022, 2, 19)},
          {'event_name': 'spain', 'event_date': date(2022, 1, 29)},
          {'event_name': 'ireland', 'event_date': date(2022, 2, 4)},
          {'event_name': 'germany', 'event_date': date(2022, 3, 4)},
          {'event_name': 'france', 'event_date': date(2022, 3, 5)},
          {'event_name': 'australia', 'event_date': date(2022, 2, 26)},
          {'event_name': 'songvakeppnin', 'event_date': date(202, 1, 1)}]
data_set_names = ['exchange_odds','spotify_song_stats', 'spotify_artist_stats', 'youtube_song_stats', 'youtube_comment_stats']
# 'youtube_artist_stats', 'spotify_artist_stats', 'twitter_posts', 'facebook_artist_stats', 'instagram_artist_stats', 'search_song_stats', 'search_artist_stats'
future_events_names = [i['event_name'] for i in events if i['event_date']>=date.today()]  #
print(future_events_names)
data_path = str(r'C:\Users\david\Documents\Betting\Models\Eurovision\Eurovision Data')
today = date.today().strftime("%d%m%Y")
youtube_api_key = 'AIzaSyB7eto6dOr9FOczpS1o_sBlO7bHSAce8uA'
#https://stackoverflow.com/questions/63211098/youtube-data-api-the-request-cannot-be-completed-because-you-have-exceeded-your
exchange_api_key = 'cVbIW2A1QobjAdHV'
spotify_client_id='f1518d69a48e4f12b09e659afc86b830'
spotify_client_secret='601ed38dcef5467fbfdd620a7d051bf8'
twitter_consumer_key = "HGtiyj9GOgJc7Dn3RCMaiQTbH"
twitter_consumer_secret = "S3JmDuhUcmvX8aEnb3kse3BDQuT3t5Bgfx107MfKySUf8VsXSW"
twitter_access_key = "2507444200-B2hIJop5O9g2zijYaIkrI89kMnyU9ZHVMOTGL4E"
twitter_access_secret = "foN3F1uqs9Pjj5fXfh7Mbh91oHEglWzQgsAvbuAxWFlEP"
google_sheets_key = 'eurovision-246416-a45a4103ec0b.json'
#all_api_ids = pd.read_excel(str(data_path + '\\Code\\IDs\\api_ids.xlsx'), sheet_name = future_events_names)
#all_market_ids = pd.read_excel(str(data_path + '\\Code\\IDs\\exchange_ids.xlsx'), sheet_name = future_events_names)
all_api_ids, all_market_ids = database_functions.read_ids(google_sheets_key, future_events_names)


# get data #

for event in future_events_names:
    ## get data from APIs ##
    api_ids = all_api_ids[event]
    market_ids = all_market_ids[event]

    youtube_song_stats, youtube_artist_stats = api_functions.youtube_api_data(youtube_api_key, contest, api_ids, data_path, today)
    youtube_comment_stats = api_functions.youtube_api_comments(youtube_api_key, contest, api_ids, data_path, today)
    exchange_odds = api_functions.exchange_api_data(exchange_api_key, contest, market_ids, data_path, today)
    exchange_odds = exchange_odds[exchange_odds['entry']!='Russia']
    exchange_odds = exchange_odds[exchange_odds['entry'] != 'Belarus']
    spotify_song_stats, spotify_artist_stats = api_functions.spotify_api_data(spotify_client_id, spotify_client_secret, contest, api_ids, data_path, today)
    #twitter_stats = api_functions.twitter_api_data(twitter_consumer_key, twitter_consumer_secret, twitter_access_key, twitter_access_secret, contest, api_ids, data_path, today)
    #facebook_artist_stats = api_functions.facebook_data()
    #instagram_artist_stats = api_functions.instagram_data()
    #search_song_stats, search_artist_stats = api_functions.search_data()

    ## QA outputs ##
    qa_results = []
    for data_set in data_set_names:
        qa_result = qa_functions_.qa_data_set(data_set, globals()[data_set])
        qa_results.append(qa_result)

    ## write outputs to Google Sheets ##
    full_data_set = pd.DataFrame({'date': [], 'entry': []})
    for data_set in range(len(qa_results)):
        if qa_results[data_set] == 1:
            full_data_set = pd.merge(full_data_set, globals()[data_set_names[data_set]], how='outer', on=['date', 'entry'], validate='one_to_one')
    full_data_set.fillna('', inplace=True)
    #full_data_set['date'] = full_data_set['date'].astype('datetime64')
    database_functions.write_data(full_data_set, google_sheets_key, contest, event)

    time.sleep(120)