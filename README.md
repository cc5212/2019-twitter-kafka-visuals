# 2019-twitter-kafka-visuals
Processing of a hashtag in realtime with Kafka to get visual information about topics, sentiment analysis and frequency

# Run instructions
- Create a virtual environment (with Python 2.7)
- Run pip install -r requirements.txt
- Install docker-compose
- Run docker-compose up (on source directory)
- Run python twitter-to-kafka.py
- Insert a hashtag (#)
- Enjoy the results! (The wordcloud lasts in screen for 10 seconds, and then it refreshes automatically every 100 tweets)

# Overview

The project consists in a Docker build, which was uploaded with Kafka. Using this, and Twitter API, we've developed a Producer/Consumer process, where every tweet is delivered from the Producer to a topic, where the Consumer can retrieve it and process it. The process consist in a preprocessing cleaning and tokenization of the tweet, then a simple sentiment analysis. Every certain (big) amount of tweets and its respective sentiment score, we go to a topic analysis process, in which we retrieve the most important topics, and the percentage indicating how much a tweet corresponds to any topic. Finally, we create a wordcloud, showing how many times a topic was named and how good/bad was its sentiment score (from green to red). Our project want to show the difference between hashtags, and how controversial hashtags (e.g #Trump) has more variety of colors, not like "peaceful" hashtags (#YogaDay2019).

# Dataset

The dataset as it was said is the raw tweets recollected from the Twitter API. We use only the content of every message received, deleting all undesired letters, symbols, and others.

# Methods

For this project, we used Docker, Python 2.7 and Kafka. We chose to develop all the code following a Producer/Consumer scheme, extensible to a lot of multi-threading consumers that could repeat the task. The principal problems were related to the techniques used. First, there is a lot of sentiment analysis tools, but they can be very imprecise, and this can be reflected in our wordcloud. Also, for topic analysis it was difficult to chose good representants of the tweets, principally because people don't give context in their tweets, only comment a single thing, so this was difficult to represent. Finally, the colours of the wordcloud were very difficult to adapt, in order to create a kind of transition between red and green, principally because the data were not as good as expected.

# Results

The results obtained show that any hashtag can be monitored in a simple way. We are very happy with the graphics because you can see real topics going on, and the colours are mostly related (looking at the printed tweets and its sentiment scores).

# Conclusion

There was a lot of stuff that we could make better, principally improving the code to get results that are useful in a certain way. Also, the technologies involved (principally Kafka and Docker) were not used at its maximum, ignoring a lot of cool features that would improved the results and the speed of the code.
