#!/usr/bin/env python

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
from kafka import KafkaProducer, KafkaConsumer
# Wordcloud
import numpy as np
import pandas as pd
from os import path
from PIL import Image
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
# Sentiment analysis
from textblob import TextBlob
# Consumer
import multiprocessing

import matplotlib.pyplot as plt
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
# Global variables
clean_tweets = []
words = ""
# Stopwords (Spanish and English)
nltk.download('stopwords')
stopWords = stopwords.words('english')
stopWords += stopwords.words('spanish')
# Number of tweets
n = 100
i = 0
# Display time
d_time = 5
# Sentiment analysis
wcdict = {}

class StdOutListener(StreamListener):
    producer = KafkaProducer(bootstrap_servers=KAFKA_ENDPOINT)

    def on_data(self, data):
        # Tweet received, send to topic
        data_json = json.loads(data)
        str_tweet = data_json['text'].encode('utf-8')
        self.producer.send(KAFKA_TOPIC, str_tweet)

    def on_error(self, status):
        print status

class Consumer(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_ENDPOINT,
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=100)
        consumer.subscribe([KAFKA_TOPIC])

        while not self.stop_event.is_set():
            for message in consumer:
                # Global variables
                global i
                global wcdict
                global words
                # Clean tweet and print
                tweet_clean = clean(message.value)
                print("-", tweet_clean)

                # Sentiment analysis
                analysis = TextBlob(tweet_clean)
                sentiment = analysis.sentiment.polarity

                # Test for words (Comentar despues)
                for t in tweet_clean.split(" "):
                    if (t != "" and t not in stopWords):
                        words += " " + t
                        if t in wcdict:
                            wcdict[t] += 1
                        else:
                            wcdict[t] = 1

                # Topic analysis
                # Obtener aca el topico de tweet_clean
                # con ese topico, que llamaremos "topic"
                # lo agregaremos al diccionario
                # topic = topicanalysis(tweet_clean)
                # if (topic != "" and topic not in stopWords):
                #   if t in wcdict:
                #       wcdict[topic] += sentiment
                #   else:
                #       wcdict[topic] = sentiment
                # Imprime el diccionario para ver que va bien
                # print(wcdict)

                # Wordcloud (Imprime cada n tweets, esperando d_time segundos)
                i += 1
                i = i % n
                # Create wordcloud each n tweets
                if i == 0:
                    # Create and generate a word cloud image:
                    wordcloud = WordCloud(stopwords=stopWords, collocations=False,
                                           colormap='plasma', background_color="white",
                                           color_func=my_tf_color_func)
                    wordcloud = wordcloud.generate_from_frequencies(wcdict)
                    # Mostrar el grafico cada 3 segundos
                    plt.imshow(wordcloud, interpolation='bilinear')
                    plt.axis("off")
                    plt.show(block=False)
                    plt.pause(d_time)
                    plt.close()
                if self.stop_event.is_set():
                    break

        consumer.close()

def my_tf_color_func(word, **kwargs):
    global wcdict
    return "hsl(%d, 80%%, 50%%)" % (360 * wcdict[word])

def clean(tweet):
    tweet = re.sub(r'[.,"!]+', '', tweet, flags=re.MULTILINE)               # removes the characters specified
    tweet = re.sub(r'^RT[\s]+', '', tweet, flags=re.MULTILINE)              # removes RT
    tweet = re.sub(r'https?:\/\/.*[\r\n]*', '', tweet, flags=re.MULTILINE)  # remove link
    tweet = re.sub(r'[:]+', '', tweet, flags=re.MULTILINE)
    tweet = filter(lambda x: x in string.printable, tweet)                  # filter non-ascii characers
    new_tweet = ''
    for i in tweet.split():  # remove @ and #words, punctuataion
        if not i.startswith('@') and not i.startswith('#') and i not in string.punctuation:
            new_tweet += i + ' '
    tweet = new_tweet
    return tweet

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

    print 'Stream mode is: %s' % TWITTER_STREAMING_MODE
    consumer = Consumer()
    consumer.start()
    stream = Stream(auth, listener)
    # set up the streaming depending upon whether our mode is 'sample', which
    # will sample the twitter public stream. If not 'sample', instead track
    # the given set of keywords.
    # This environment var is set in the 'twitter-stream.yaml' file.
    if TWITTER_STREAMING_MODE == 'sample':
        stream.sample()
    else:
        stream.filter(track=["#"+TWITTER_TEXT_FILTER])

    time.sleep(300)
    producer.stop()
    consumer.stop()
    producer.join()
    consumer.join()
