#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python3
import os
import sys
import praw
import config
import datetime as dt
import time
from config import time_to_mysql, time_from_mysql
from dateutil import parser
from connect import reddit_api_login
import database
import json
import subprocess
import mysql.connector
from mysql.connector import Error
from twitter_streamer import heartbeat,process_id,fetch_data,duplicate,similarity_score,text_to_vector,get_cosine,text_cleaner,lemmatizer,tokenization,remove_punct
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd
import nltk
from nltk.stem import PorterStemmer
porter = PorterStemmer()
nltk.download('wordnet')
wn = nltk.WordNetLemmatizer()
predefined_alerts=pd.read_csv('/var/www/sentiment/app/Predefined.csv')

reddit = praw.Reddit(client_id='yAWmQCDocxAonw',client_secret='1FknRtWIhfE2pU-cynjUbb0ct1k',
                     username='slippast'
                     ,password='7FYrGg6eJzcjNtQz'
                     ,user_agent='web:sentiment_analysis_project:0.1 (by /u/slippast)')

def connect(body, parent_id, source_id, timestamp):
    """
    connect to MySQL database and insert twitter data
    """
    try:
        con = mysql.connector.connect(host='localhost',
                                      database='sentiment', user='sentiment', password='sentiment', charset='utf8mb4')

        if con.is_connected():
            """
            Insert twitter data
            """
            cursor = con.cursor(buffered=True)
            # twitter, golf
            query = "INSERT INTO reddit_record (body, parent_id, source_id, time_of_the_day) VALUES (%s, %s,%s, %s)"
            cursor.execute(query,
                           (body, parent_id, source_id, timestamp))
            con.commit()

    except Error as e:
        print(e)

    cursor.close()
    con.close()

    return


def filter_comment(comment):
    filter = False
    useful_keywords = ['load', 'work', 'network', 'buffer', 'ingest', 'broken', 'connect', 'lag', 'offline', 'watch',
                       'buffer', 'blank', 'black', 'freezing', 'segments', 'website', 'quality', 'rip', 'syn1c', 'live',
                       'stutter', 'down', 'play']
    useless_keywords = ['hack','ban','cheat','block','spam','suspension']
    for word in useful_keywords:
        if word in comment:
            filter = True
    for word in useless_keywords:
        if word in comment:
            filter = False
    return filter


def analyse_sentiment(sentence):
    analyser = SentimentIntensityAnalyzer()
    score = analyser.polarity_scores(sentence)
    return score


def connect_sentiment(text, score, handle, source_id, timestamp):
    try:
        con = mysql.connector.connect(host='localhost',
                                      database='sentiment', user='sentiment', password='sentiment', charset='utf8mb4')

        if con.is_connected():
            """
            Insert twitter data
            """
            cursor = con.cursor(buffered=True)
            # twitter, golf
            query = "INSERT INTO sentiment_record (opinion_text,negative_score,social_handle,opinion_id,time_of_the_day) VALUES (%s,%s, %s,%s,%s)"
            cursor.execute(query,
                           (text, score, handle, source_id, timestamp))
            con.commit()

    except Error as e:
        print(e)

    cursor.close()
    con.close()

    return

class redditStreamer():
    def on_data(self):
        subreddit = reddit.subreddit('Twitch')
        print("Connected to reddit Stream")
        counter=0
        while True:
            for comment in subreddit.stream.comments():
                try:
                    if counter>100:
                        heartbeat("reddit_comments")
                        created=str(dt.datetime.now())
                        connect(comment.body, comment.parent_id, comment.id, created)
                        print("post collected at:"+str(created))
                        if filter_comment(comment.body):
                            if similarity_score(comment.body)>0.3:
                                if duplicate(comment.body):
                                    comment.body = text_cleaner(comment.body)
                                    sentiment = analyse_sentiment(comment.body)['neg']
                                    connect_sentiment(comment.body, sentiment, 'Reddit', comment.id, created)
                                    print("Post collected in sentiment_record at: {} ".format(str(created)))
                    else: counter+=1
                except praw.exceptions.PRAWException as e:
                    print('failed ondata' + str(e))

if __name__=='__main__':
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
    pid = os.getpid()
    process_id(pid, "reddit_comments")
    redditStream = redditStreamer()
    redditStream.on_data()













