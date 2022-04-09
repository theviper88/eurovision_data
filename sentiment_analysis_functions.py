from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pandas.io.json import json_normalize

def sentiment_analysis(comments):
    douze_point_strings = ['12points', 'twelvepoints', 'douzepoints']
    #any cleaning required? - https://claritynlp.readthedocs.io/en/latest/developer_guide/algorithms/sentence_tokenization.html
    analyser = SentimentIntensityAnalyzer()
    comments['sentiment_results'] = comments['text'].apply(analyser.polarity_scores)
    comments['sentiment'] = [i['compound'] for i in comments['sentiment_results']]
    comments['douze_points'] = [1 if any(j in i.replace(" ", "").lower() for j in douze_point_strings) else 0 for i in comments['text']]
    return comments


def get_sentiment_metrics(comments):
    comments['positive_sentiment'] = [max(0,i) for i in comments['sentiment']]
    comments['weighted_sentiment'] = comments['sentiment'] * (comments['likes']+1)
    comments['weighted_positive_sentiment'] = comments['positive_sentiment'] * (comments['likes']+1)
    comments['love'] = [1 if i > 0.8 else 0 for i in comments['positive_sentiment']]
    comments['weighted_love'] = [j+1 if i > 0.8 else 0 for i,j in zip(comments['positive_sentiment'], comments['likes'])]
    comments['weighted_douze_points'] = comments['douze_points'] * (comments['likes']+1)
    return comments

