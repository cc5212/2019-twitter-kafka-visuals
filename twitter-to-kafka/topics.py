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
import preprocessor
import simplejson as json
import time
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from kafka import KafkaProducer
from os import path
from PIL import Image
# LDA topics
import gensim
from gensim import corpora, models
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS
from nltk.stem import WordNetLemmatizer, SnowballStemmer
from nltk.stem.porter import *

import matplotlib.pyplot as plt
import numpy as np
import re
import string
from nltk.corpus import stopwords
import nltk
nltk.download("wordnet")

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
        global dictionary
        """What to do when tweet data is received."""
        data_json = json.loads(data)
        str_tweet = data_json['text'].encode('utf-8')
        self.producer.send(KAFKA_TOPIC, str_tweet)
        print("-", str_tweet)
        tweet_clean = preprocess(str_tweet)
        print("-", tweet_clean)
        clean_tweets.append(tweet_clean)

        # Create bag of words
        dictionary = gensim.corpora.Dictionary(clean_tweets)
        bow = [dictionary.doc2bow(tweet) for tweet in clean_tweets]

        # TF-IDF
        tfidf = models.TfidfModel(bow)
        tweets_tfidf = tfidf[bow]

        # Extract LDA topics
        if len(clean_tweets) % 10 == 0 and len(clean_tweets) > 0 :
            num_topics = 10
            lda_model_tfidf = gensim.models.LdaMulticore(tweets_tfidf, num_topics=num_topics, id2word=dictionary, passes=2, workers=4)

            for idx, topic in lda_model_tfidf.print_topics(-1):
                print('Topic: {} Word: {}'.format(idx, topic))
        time.sleep(3)

    def on_error(self, status):
        print status

def preprocess(tweet):
    '''
    Cleans tweet, removes stopwords. Performs tokenization,
    lemmatization and stemming.
    '''
    cleaned_tweet = preprocessor.clean(tweet)  # Cleans tweet
    processed_tweet = []
    stemmer = SnowballStemmer("english")
    for token in gensim.utils.simple_preprocess(cleaned_tweet):
        if token not in gensim.parsing.preprocessing.STOPWORDS and len(token) > 3:
            processed_tweet.append(stemmer.stem(WordNetLemmatizer().lemmatize(token, pos='v')))

    return processed_tweet


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
