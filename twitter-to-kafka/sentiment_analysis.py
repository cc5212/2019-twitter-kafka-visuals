#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""This script uses the Twitter Streaming API, via the tweepy library,
to pull in tweets and publish them to a Kafka topic.
"""

import base64
import datetime
import os
import logging
import ConfigParser
import simplejson as json
import time
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from kafka import KafkaProducer
from os import path
from PIL import Image
# Sentiment analysis
from textblob import TextBlob

import numpy as np
import re
import string
from nltk.corpus import stopwords
import nltk

# Get your twitter credentials from the environment variables.
# These are set in the 'twitter-stream.json' manifest file.

config = ConfigParser.RawConfigParser()
config.read('config.cfg')

CONSUMER_KEY = config.get('Twitter', 'consumer_key')
CONSUMER_SECRET = config.get('Twitter', 'consumer_secret')
ACCESS_TOKEN = config.get('Twitter', 'access_token')
ACCESS_TOKEN_SECRET = config.get('Twitter', 'access_token_secret')
TWITTER_STREAMING_MODE = config.get('Twitter', 'streaming_mode')
KAFKA_ENDPOINT = '{0}:{1}'.format(config.get('Kafka', 'kafka_endpoint'), config.get('Kafka', 'kafka_endpoint_port'))
KAFKA_TOPIC = config.get('Kafka', 'topic')
NUM_RETRIES = 3
words = ""
clean_tweets = []
nltk.download('stopwords')
stopWords = stopwords.words('english')
stopWords += stopwords.words('spanish')

class StdOutListener(StreamListener):
    """A listener handles tweets that are received from the stream.
    This listener dumps the tweets into a Kafka topic
    """

    producer = KafkaProducer(bootstrap_servers=KAFKA_ENDPOINT)

    def on_data(self, data):
        global words
        global clean_tweets
        """What to do when tweet data is received."""
        data_json = json.loads(data)
        str_tweet = data_json['text'].encode('utf-8')
        self.producer.send(KAFKA_TOPIC, str_tweet)
        #print("-", str_tweet)
        tweet_clean = clean(str_tweet)
        #print("-", tweet_clean)
        words = words + tweet_clean
        clean_tweets.append(tweet_clean)

        # Sentiment analysis
        k = 1
        positive_tweets, negative_tweets = sentiment_analysis(clean_tweets, k)
        if len(positive_tweets) >= k and len(negative_tweets) >= k:
            print
            print("Top {} positive tweets:".format(k))
            for i in range(k):
                print("-", positive_tweets[i])
            print("Top {} negative tweets:".format(k))
            for i in range(k):
                print("-", negative_tweets[i])

                
    def on_error(self, status):
        print status

def clean(tweet):
    tweet = re.sub(r'[.,"!]+', '', tweet, flags=re.MULTILINE)               # removes the characters specified
    tweet = re.sub(r'^RT[\s]+', '', tweet, flags=re.MULTILINE)              # removes RT
    tweet = re.sub(r'https?:\/\/.*[\r\n]*', '', tweet, flags=re.MULTILINE)  # remove link
    tweet = re.sub(r'[:]+', '', tweet, flags=re.MULTILINE)
    tweet = filter(lambda x: x in string.printable, tweet)                  # filter non-ascii characters
    new_tweet = ''
    for i in tweet.split():  # remove @ and #words, punctuataion
        if not i.startswith('@') and not i.startswith('#') and i not in string.punctuation:
            new_tweet += i + ' '
    tweet = new_tweet
    return tweet


def sentiment_analysis(tweets, k):
    ''' 
    Returns top-k positive/negative tweets based on polarity score
    '''
    polarities = []
    positive_tweets = []
    negative_tweets = []
    for tweet in tweets:
        analysis = TextBlob(tweet) 
        polarities.append(analysis.sentiment.polarity)
    polarities_arr = np.array(polarities)
    sorted_indices = np.argsort(polarities_arr)
    number_of_pos = len(polarities_arr[polarities_arr > 0])
    number_of_neg = len(polarities_arr[polarities_arr < 0])

    print
    print("----------------------------------------------------")
    print
    print("Total number of tweets: {}".format(len(tweets)))
    print("Percentage of positive tweets: {0:.2f}%".format(number_of_pos * 100.0 / len(tweets)))
    print("Percentage of negative tweets: {0:.2f}%".format(number_of_neg * 100.0 / len(tweets)))
    
    if number_of_pos >= k and number_of_neg >= k:
        for i in range(k):
            positive_tweets.append(tweets[sorted_indices[::-1][i]])
            negative_tweets.append(tweets[sorted_indices[i]])

    return positive_tweets, negative_tweets


if __name__ == '__main__':
    TWITTER_TEXT_FILTER = raw_input("Inserte su hashtag: #")
    stopWords += re.findall('[A-Z][^A-Z]*', TWITTER_TEXT_FILTER)
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )

    listener = StdOutListener()
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    print 'stream mode is: %s' % TWITTER_STREAMING_MODE

    stream = Stream(auth, listener)
    # set up the streaming depending upon whether our mode is 'sample', which
    # will sample the twitter public stream. If not 'sample', instead track
    # the given set of keywords.
    # This environment var is set in the 'twitter-stream.yaml' file.
    if TWITTER_STREAMING_MODE == 'sample':
        stream.sample()
    else:
        stream.filter(track=["#"+TWITTER_TEXT_FILTER], languages=['en'], encoding='utf-8')
