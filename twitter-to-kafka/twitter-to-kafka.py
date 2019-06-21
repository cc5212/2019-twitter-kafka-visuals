#!/usr/bin/env python

"""This script uses the Twitter Streaming API, via the tweepy library,
to pull in tweets and publish them to a Kafka topic.
"""

# Twitter API
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
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from nltk.corpus import stopwords
import nltk
import re
import string
# Sentiment analysis
from textblob import TextBlob
# Consumer
import multiprocessing
# LDA topics
import gensim
from gensim import corpora, models
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS
from nltk.stem import WordNetLemmatizer, SnowballStemmer
from nltk.stem.porter import *

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
tweets_sentiment = []
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
# Global frequencies
wcfreq = {}


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
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_ENDPOINT, consumer_timeout_ms=100)
        consumer.subscribe([KAFKA_TOPIC])

        while not self.stop_event.is_set():
            for message in consumer:
                # Global variables
                global i
                global wcdict
                global wcfreq
                global words
                # Clean tweet
                tweet_clean = clean(message.value)
                processed_tweet = preprocess(message.value)
                clean_tweets.append(processed_tweet)

                # Sentiment analysis
                analysis = TextBlob(tweet_clean)
                sentiment = analysis.sentiment.polarity
                tweets_sentiment.append(sentiment)

                # Print tweet and sentiment
                #print("-", sentiment, tweet_clean)
                print("{} - {}".format(len(clean_tweets), tweet_clean))

                # Extract topics
                if len(clean_tweets) > 0 and len(clean_tweets) % n == 0:
                    wcdict, wcfreq = topic_analysis(clean_tweets, 20, 3, tweets_sentiment)
                    # Wordcloud (Imprime cada n tweets, esperando d_time segundos)
                    # Create and generate a word cloud image:
                    wordcloud = WordCloud(stopwords=stopWords, collocations=False, background_color="white",
                                           color_func=my_tf_color_func)
                    wordcloud = wordcloud.generate_from_frequencies(wcfreq)
                    plt.imshow(wordcloud, interpolation='bilinear')
                    plt.axis("off")
                    plt.show(block=False)
                    plt.pause(d_time)
                    plt.close()
                # Test for words (Comentar despues)
                #for t in tweet_clean.split(" "):
                #    if (t != "" and t not in stopWords):
                #        words += " " + t
                #        if t in wcfreq:
                #            wcfreq[t] += 1
                #        else:
                #            wcfreq[t] = 1
                #        if t in wcdict:
                #            wcdict[t] += sentiment
                #        else:
                #            wcdict[t] = sentiment

                if self.stop_event.is_set():
                    break
        consumer.close()

def my_tf_color_func(word, **kwargs):
    global wcdict
    global wcfreq
    act_value = wcdict[word] / wcfreq[word]
    norm_value = (act_value + 1) / 2
    return "hsl(%d, 80%%, 50%%)" % (120 * norm_value)

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

def topic_analysis(processed_tweets, num_topics, words_by_topic, sentiment_list):
    '''
    Performs LDA analysis. Returns two dictionaries:
    wcdict: keys are words_by_topic most relevant words for each topic; values are given by the
    sentiment score of each tweet multiplied by the relevance of that
    topic in the tweet.
    wcfreq: keys are the same as before. Values are the frequency of topic words in all tweets.
    '''
    # Create bag of words
    dictionary = gensim.corpora.Dictionary(processed_tweets)
    bow = [dictionary.doc2bow(tweet) for tweet in processed_tweets]
    # TF-IDF
    tfidf = models.TfidfModel(bow)
    tweets_tfidf = tfidf[bow]
    # Extract LDA topics
    lda_model_tfidf = gensim.models.LdaMulticore(tweets_tfidf, num_topics=num_topics, id2word=dictionary, passes=2, workers=4)
    # Create dictionaries
    topics_names = []
    wcdict = {}
    wcfreq = {}
    for i in range(num_topics):
        # Topic name will be its topn words
        topic_name = ''
        for elem in lda_model_tfidf.show_topic(topicid=i, topn=words_by_topic):
            topic_name += elem[0] + '-'
        topic_name = topic_name[:-1]  # Remove last -
        topics_names.append(topic_name)
        wcdict[topic_name] = 0
        wcfreq[topic_name] = 0

    # Iterate over tweets
    for idx, tweet in enumerate(processed_tweets):
        tweet_topics = lda_model_tfidf.get_document_topics(bow[idx])  # (topic, relevance)
        for elem in tweet_topics:
            wcdict[topics_names[elem[0]]] += elem[1] * sentiment_list[idx]
        for word in tweet:
            for idx2, topic_name in enumerate(topics_names):
                if word in topic_name:
                    wcfreq[topics_names[idx2]] += 1

    return wcdict, wcfreq


def preprocess(tweet):
    '''
    Cleans tweet, removes stopwords. Performs tokenization,
    lemmatization and stemming.
    '''
    #cleaned_tweet = preprocessor.clean(tweet)  # Cleans tweet
    cleaned_tweet = clean(tweet)
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
    listener.stop()
    consumer.stop()
    listener.join()
    consumer.join()
