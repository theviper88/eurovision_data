import pandas as pd
from pandas.io.json import json_normalize
from datetime import date
import re
import functools
import requests
import urllib
import json
import math


def exchange_connect(username, password, api_key):        
    payload = 'username='+username+'&password='+password
    headers = {'X-Application': api_key,'Content-Type': 'application/x-www-form-urlencoded'}
    resp = requests.post('https://identitysso-cert.betfair.com/api/certlogin', data=payload, cert=('client-2048.crt', 'client-2048.key'), headers=headers) 
    resp_json = resp.json()
    if resp_json['loginStatus'] == 'SUCCESS':
      return resp_json['sessionToken']

def callAping(jsonrpc_req, appKey, sessionToken):
    headers = {'X-Application': appKey, 'X-Authentication': sessionToken, 'content-type': 'application/json'}
    url = "https://api.betfair.com/exchange/betting/json-rpc/v1"
    try:
        req = urllib.request.Request(url, jsonrpc_req.encode('utf-8'), headers)
        response = urllib.request.urlopen(req)  #######error#######
        jsonResponse = response.read()
        return jsonResponse.decode('utf-8')
    except urllib.error.URLError as e:
        print (e.reason) 
        print ('Oops no service available at ' + str(url))
        exit()
    except urllib.error.HTTPError:
        print ('Oops not a valid operation from the service ' + str(url))
        exit()
        
def getMarketDeets(appKey, sessionToken, marketID):
    market_catalogue_req = '{"jsonrpc": "2.0","method": "SportsAPING/v1.0/listMarketCatalogue","params": {"filter": {"marketIds": ["'+ marketID +'"]}, "maxResults": "1","marketProjection":["RUNNER_METADATA"]},"id": 1}' 
    market_catalogue_response = callAping(market_catalogue_req, appKey, sessionToken)
    decodedCatalogue = json.loads(market_catalogue_response)
    return decodedCatalogue

def getSelections(appKey, sessionToken, marketID):
   market_book_req = '{"jsonrpc": "2.0","method": "SportsAPING/v1.0/listMarketBook","params": {"marketIds": ["'+ marketID +'"],"priceProjection": {"priceData": ["EX_BEST_OFFERS","EX_TRADED","SP_AVAILABLE"],"virtualise": "true"}},"id": 1}' 
   market_book_response = callAping(market_book_req, appKey, sessionToken)
   decodedSelections = json.loads(market_book_response)
   try:
       selectionsResult = decodedSelections['result']
       return selectionsResult
   except:
       print('Exception from API-NG' + str(selectionsResult['error']))

def get_exchange_odds(contest, marketIDs, appKey):
    sessionToken = exchange_connect('Mirrornandi', 'Mirrorrush1', appKey)
    marketIDs = marketIDs[marketIDs.year == contest]
    marketIDs = marketIDs.drop(columns=['year']).reset_index(drop=True).fillna(0).replace('',0).astype(int)
    market_prices = []
    for market in marketIDs:
        if marketIDs[market][0] > 0: #not math.isnan(marketIDs[market][0]):
            ##define market##
            market_name = re.sub('\_id$', '', market)
            marketID = '1.'+str(marketIDs[market][0])
            marketDeets = getMarketDeets(appKey, sessionToken, marketID)
            runners = json_normalize(marketDeets['result'][0]['runners'])
            ##get market data##
            marketBook = getSelections(appKey, sessionToken, marketID)
            prices = json_normalize(marketBook[0]['runners'])
            if 'lastPriceTraded' not in prices.columns:
                prices['lastPriceTraded'] = None
            prices[market_name+'_back_odds'] = ['' if len(prices['ex.availableToBack'][i])==0 else prices['ex.availableToBack'][i][0]['price'] for i in range(len(prices))]
            prices[market_name+'_lay_odds'] = ['' if len(prices['ex.availableToLay'][i])==0 else prices['ex.availableToLay'][i][0]['price'] for i in range(len(prices))]
            #prices[market_name+'_traded_volume'] = [0 if len(prices['ex.tradedVolume'][i])==0 else sum(prices['ex.tradedVolume'][i][j]['size'] for j in range(len(prices['ex.tradedVolume'][i]))] for i in range(len(prices))]
            ##combine all prices##
            runner_prices = runners.merge(prices, on='selectionId')
            runner_prices = runner_prices[['runnerName', 'lastPriceTraded', market_name+'_back_odds', market_name+'_lay_odds', 'totalMatched']]
            runner_prices = runner_prices.rename(columns={"runnerName": "entry", "lastPriceTraded": market_name+"_last_matched_odds", "totalMatched": market_name+"_traded_volume"})
            market_prices.append(runner_prices)
    ##combine all markets##
    if len(market_prices) > 0:
        all_odds = functools.reduce(lambda x, y: pd.merge(x, y, on = 'entry', how='outer'), market_prices)
        all_odds.insert(0, 'date', [date.today().strftime("%d%m%Y")]*len(all_odds))
    else:
        all_odds = ''
    return all_odds
        

