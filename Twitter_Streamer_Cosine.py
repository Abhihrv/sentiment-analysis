#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python3
import json
from dateutil import parser
import time
import os
import subprocess
import mysql.connector
import pyodbc
from mysql.connector import Error
import tweepy
import time
import sys
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from config import timestamp_unix, time_to_mysql, time_from_mysql
import pandas as pd
from nltk.stem import PorterStemmer
import nltk
import string
import re
porter = PorterStemmer()
nltk.download('wordnet')
wn = nltk.WordNetLemmatizer()
predefined_alerts=pd.read_csv('/var/www/sentiment/app/Predefined.csv')

'''
functions for all the cleaning processes
'''
def remove_punct(text):
    text = "".join([char for char in text if char not in string.punctuation])
    text = re.sub('[0-9]+', ' ', text)
    return text

def tokenization(text):
    text = re.split('\W+', text)
    return text

def lemmatizer(text):
    text = [wn.lemmatize(word, pos="v") for word in text]
    return text


def text_cleaner(post):
    post = remove_punct(post)
    post = post.replace('\n', " ").strip()
    post = re.sub(r"http\S+", " ", post)
    post = re.sub("\d+", " ", post)
    post = re.sub(r'[^\x00-\x7F]+', ' ', post)
    punctuation_filtered_tokens = tokenization(post.lower())
    stem_tokens=[porter.stem(item) for item in punctuation_filtered_tokens]
    lemma_tokens = [lemmatizer(item) for item in stem_tokens]
    final_string=" ".join(lemma_tokens)
    return final_string

'''
First, to authenticate the twitter API call, 4 parameters are to be defined which are given below
'''
CONSUMER_KEY = '33Y3gcaIUsk6PQKhSK7l57Svr'
CONSUMER_SECRET = 'smOshPax4lLv16NtJFLdP3O9vUxhxza2A6KIDROImy55Ao2m91'
OAUTH_TOKEN = '74972299-c9TCEnw3hEOQNFpWQzdPLYwSQbQ6Uyfo2lo0SMwkn'
OAUTH_TOKEN_SECRET = '4F4EqxlsW6RQXGhBadUMKv4xTy5VNx2VDG6qX7MgKRFwk'

'''
Now onto authenticating the API
'''
auth = tweepy.auth.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
api = tweepy.API(auth)

'''
Cosine function
'''
def get_cosine(vec1, vec2):
    intersection = set(vec1.keys()) & set(vec2.keys())
    numerator = sum([vec1[x] * vec2[x] for x in intersection])

    sum1 = sum([vec1[x] ** 2 for x in vec1.keys()])
    sum2 = sum([vec2[x] ** 2 for x in vec2.keys()])
    denominator = math.sqrt(sum1) * math.sqrt(sum2)

    if not denominator:
        return 0.0
    else:
        return float(numerator) / denominator

'''
Word to Vector
'''
def text_to_vector(text):
    WORD = re.compile(r'\w+')
    words = WORD.findall(text)
    return Counter(words)
'''
Function to calculate similarity in two sentences
'''
def similarity_score(post):
    post = text_cleaner(post)
    vector_alert=text_to_vector(post)
    scores=list()
    for item in predefined_alerts[0]:
        vector_pre = text_to_vector(item)
        cosine = get_cosine(vector_alert, vector_pre)
        scores.append(cosine)
    return max(scores)

'''
Fetching all data for the current data
'''
def fetch_data():
    try:
        con = mysql.connector.connect(host='localhost',
                                      database='sentiment', user='sentiment', password='sentiment', charset='utf8mb4')

        if con.is_connected():
            """
            Insert twitter data
            """
            cursor = con.cursor(buffered=True)

            query = "SELECT opinion_text,time_of_the_day FROM sentiment_record WHERE time_of_the_day > now()-INTERVAL 24 HOUR"
            cursor.execute(query)
            record=cursor.fetchall()
            con.commit()

    except Error as e:
        print(e)

    cursor.close()
    con.close()

    return record

'''
The heartbeat function will be used to monitor the bot
'''
def heartbeat(platform):
    try:
        con = mysql.connector.connect(host='localhost',
                                      database='sentiment', user='sentiment', password='sentiment', charset='utf8mb4')
        if con.is_connected():
            cursor_heartbeat = con.cursor(buffered=True)
            query = "SELECT * from bot_tracker where platform =%s"
            cursor_heartbeat.execute(query,(platform,))
            bot_tracker = cursor_heartbeat.fetchone()
            heartbeat_two = bot_tracker[3]
            heartbeat_one = time_to_mysql()
            query = "UPDATE bot_tracker set heartbeat_one=%s , heartbeat_two=%s WHERE platform=%s"
            cursor_heartbeat.execute(query , (heartbeat_one , heartbeat_two,platform))
            con.commit()
            print(bot_tracker[3])
            cursor_heartbeat.close()
            con.close()
    except Error as e:
        print(e)

'''
Defining the process_id function to store the process_id which can be used to kill the bot and restart it
'''
def process_id(pid,platform):
    try:
        con = mysql.connector.connect(host='localhost',database='sentiment', user='sentiment', password='sentiment', charset='utf8mb4')
        if con.is_connected():
            cursor_process = con.cursor(buffered=True)
            started=time_to_mysql()
            state = 1
            query = "UPDATE bot_tracker set started=%s,state =%s, pid=%s WHERE platform=%s"
            cursor_process.execute(query, (started,state, pid,platform))
            con.commit()
            cursor_process.close()
            con.close()
    except Error as e:
        print(e)


"""
connect to MySQL database and insert twitter data
"""
def insert_tweet(tweet_id, username, timestamp, text, followers_count, friends_count, lang, place, location):

    try:
        con = mysql.connector.connect(host='localhost',
                                     database='sentiment', user='sentiment', password='sentiment', charset='utf8mb4')

        if con.is_connected():
            """
            Insert twitter data
            """
            cursor_insert_tweet = con.cursor(buffered=True)
            # twitter, golf
            query = "INSERT INTO twitter_record (tweet_id,username,time_of_the_day, tweet_text, followers_count, friends_count, lang, place, location) VALUES (%s,%s, %s, %s, %s, %s, %s,%s, %s)"
            cursor_insert_tweet.execute(query,
                           (tweet_id, username, timestamp, text, followers_count, friends_count, lang, place, location))
            con.commit()
            cursor_insert_tweet.close()

    except Error as e:
        print(e)

    return

"""
Filtering the tweets as per the keywords
"""
def filter_tweet(tweet):
    filter=False
    useful_keywords=['load', 'work','network' ,'buffer', 'ingest', 'broken', 'connect', 'lag', 'offline', 'watch', 'buffer', 'blank', 'black', 'freezing', 'segments', 'website', 'quality', 'rip', 'syn1c', 'live', 'stutter', 'down', 'play']
    useless_keywords=['hack','ban','cheat','block','spam','suspension']
    for word in useful_keywords:
        if word in tweet:
            filter=True
    for word in useless_keywords:
        if word in tweet:
            filter=False
    return filter

'''
Analysing the sentiment of each tweet using the customized vader sentiment model
'''

def analyse_sentiment(sentence):
    updated_words = {'stream': -1,
                     'load': -1,
                     'buffer': -1,
                     'break': -1,
                     'lag': -1,
                     'issue': -1,
                     'network': -1,
                     'problem': -1,
                     'work': -1,
                     'ingest': -0.4,
                     'broken': -1,
                     'connect': -1,
                     'blank': -1,
                     'black': -1,
                     'freeze': -1,
                     'segments': -0.5,
                     'website': -1,
                     'quality': -1,
                     'down': -0.5,
                     'rip': -0.5,
                     'live': -1,
                     'stutter': -1
                     }
    analyser.lexicon.update(updated_words)
    score = analyser.polarity_scores(sentence)
    return score

'''
Duplication search
'''
def duplicate(text):
    old_data = fetch_data()
    counter=True
    if(len(old_data)!=0):
        old_data=pd.DataFrame(old_data)
        for item in list(old_data[0]):
            if item == text:
                counter=False
                print('Opinion already exists in the sentiment record')
                break
    else: return True

    return counter



'''
Storing the sentiment after filtering the tweet and getting a sentiment record
'''
def connect_sentiment(text, score, handle,tweet_id,timestamp):
    try:
        con = mysql.connector.connect(host='localhost',
                                      database='sentiment', user='sentiment', password='sentiment', charset='utf8mb4')

        if con.is_connected():
            """
            Insert twitter data
            """
            cursor_connect_sentiment = con.cursor(buffered=True)
            # twitter, golf
            query = "INSERT INTO sentiment_record (opinion_text,negative_score,social_handle,opinion_id,time_of_the_day) VALUES (%s,%s, %s,%s,%s)"
            cursor_connect_sentiment.execute(query,
                           (text, score, handle,tweet_id,timestamp))
            con.commit()
            cursor_connect_sentiment.close()
            con.close()

    except Error as e:
        print(e)
    return

'''
Defining the stream class to run the twitter stream
'''
class MyStreamListener(tweepy.StreamListener):
    def on_connect(self):
        print("You are connected to the Twitter API")

    def on_data(self, data):

        try:
            heartbeat("twitter")
            raw_data = json.loads(data)
            if 'text' in raw_data:
                if 'extended_tweet' in raw_data:
                    print(raw_data['extended_tweet']['full_text'])
                    tweet = raw_data['extended_tweet']['full_text']
                else:
                    print(raw_data['text'])
                    tweet = raw_data['text']
                tweetid = raw_data['id']
                username = raw_data['user']['screen_name']
                created_at = parser.parse(raw_data['created_at'])
                followerCount = raw_data['user']['followers_count']
                friendsCount = raw_data['user']['friends_count']
                lang = raw_data['lang']
                if raw_data['place'] is not None:
                    place = raw_data['place']['country']
                    #print(place)
                else:
                    place = "NA"
                location = raw_data['user']['location']
                insert_tweet(tweetid, username, created_at, tweet, followerCount, friendsCount, lang, place, location)
                if filter_tweet(tweet):
                    if similarity_score(tweet)>0.3:
                        tweet=text_cleaner(tweet)
                        sentiment = analyse_sentiment(tweet)['neg']
                        if duplicate(tweet):
                            connect_sentiment(tweet, sentiment, 'Twitter',tweetid,created_at)
                            print("Tweet collected in sentiment_record at: {} ".format(str(created_at)))# Use this line to check if the tweets are being collected in the database or not

        except BaseException as e:
            print('failed ondata' + str(e))
            time.sleep(5)

    def on_error(self, status):
        print(status)

if __name__=="__main__":
    analyser = SentimentIntensityAnalyzer()
    updated_words = {'stream': -1,
                     'load': -1,
                     'buffer': -1,
                     'break': -1,
                     'lag': -1,
                     'issue': -1,
                     'network': -1,
                     'problem': -1,
                     'work': -1,
                     'ingest': -0.4,
                     'broken': -1,
                     'connect': -1,
                     'blank': -1,
                     'black': -1,
                     'freeze': -1,
                     'segments': -0.5,
                     'website': -1,
                     'quality': -1,
                     'down': -0.5,
                     'rip': -0.5,
                     'live': -1,
                     'stutter': -1
                     }
    analyser.lexicon.update(updated_words)
    pid=os.getpid()
    process_id(pid,"twitter")
    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
    myStream.filter(track=['TwitchSupport'])

