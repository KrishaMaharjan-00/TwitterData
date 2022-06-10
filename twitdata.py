import tweepy
import pyspark
import os
from dotenv import load_dotenv
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime


def twitterAuthentication():
    ''' Setting Up Twitter Authentication'''
    # Calling api
    bearer_token = os.getenv('BEARER_TOKEN')
    client = tweepy.Client(
        bearer_token=bearer_token)
    return client


def get_usertweets(client, wk_ago, d_before):
    ''' Getting User Search Tweets'''
    tweetdata = []
    # Search query
    query = 'billgates lang:en -is:retweet'
    for tweet in tweepy.Paginator(client.search_recent_tweets, query=query, start_time=wk_ago, end_time=d_before,
                                  tweet_fields=['author_id', 'created_at', 'public_metrics', 'referenced_tweets'], max_results=100).flatten(limit=1000):
        id = tweet.id
        text = tweet.text
        author_id = tweet.author_id
        created_at = tweet.created_at.date()
        public_metrics = tweet.public_metrics
        public_metrics_values = list(public_metrics.values())
        count_retweet = public_metrics_values[0]
        count_reply = public_metrics_values[1]
        count_like = public_metrics_values[2]
        count_quote = public_metrics_values[3]
        tweetdata.append([id, text, author_id, created_at, count_retweet,
                          count_reply, count_like, count_quote])

    return tweetdata


def get_timelinetweets(client, id, wk_ago, d_before):
    ''' Getting User Timeline Tweets '''
    tweetdata = []
    for tweet in tweepy.Paginator(client.get_users_tweets, id=id, start_time=wk_ago, end_time=d_before,
                                  tweet_fields=['author_id', 'created_at', 'public_metrics', 'referenced_tweets'], max_results=100).flatten(limit=1000):
        id = tweet.id
        text = tweet.text
        author_id = tweet.author_id
        created_at = tweet.created_at.date()
        public_metrics = tweet.public_metrics
        public_metrics_values = list(public_metrics.values())
        count_retweet = public_metrics_values[0]
        count_reply = public_metrics_values[1]
        count_like = public_metrics_values[2]
        count_quote = public_metrics_values[3]
        tweetdata.append([id, text, author_id, created_at, count_retweet,
                          count_reply, count_like, count_quote])

    return tweetdata


def twitdata_dataframe(twitdata):
    ''' Twitter Data Into Dataframe'''
    schema = StructType([StructField("id", StringType(), True),
                         StructField("text", StringType(), True),
                         StructField("author_id", StringType(), True),
                         StructField("created_at", DateType(), True),
                         StructField("count_retweet",
                                     IntegerType(), True),
                         StructField("count_reply", IntegerType(), True),
                         StructField("count_like", IntegerType(), True),
                         StructField("count_quote", IntegerType(), True)])
    df = spark.createDataFrame(twitdata, schema)
    return(df)


# Write data into MongoDB

def writeIntoMongo(df, db, coll):
    ''' Saving Into MongoDb '''
    df.write\
        .option("spark.mongodb.output.uri",
                f"mongodb://localhost:27017/{db}.{coll}").format("com.mongodb.spark.sql.DefaultSource")\
        .mode("overwrite").save()


def readFromMongo(db, coll):
    ''' Reading From MongoDb '''
    df = spark.read.option("spark.mongodb.input.uri",
                           f"mongodb://localhost:27017/{db}.{coll}?readPreference=primaryPreferred")\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .load()
    return df


if __name__ == '__main__':
    load_dotenv('./ElevatedTwitData/credentials.env')
    path = os.getenv('DRIVER_PATH')
    sc = SparkContext()
    spark = SparkSession.builder.master("local[*]").appName("TwitData")\
        .config("spark.driver.extraClassPath", path) \
        .getOrCreate()
    client = twitterAuthentication()
    week_ago = datetime.datetime.now() - datetime.timedelta(days=7)
    day_before = datetime.datetime.now() - datetime.timedelta(days=1)
    search_data = get_usertweets(client, week_ago, day_before)
    user_id = os.getenv('USER_ID')
    timeline_data = get_timelinetweets(client, user_id, week_ago, day_before)
    search_data_df = twitdata_dataframe(search_data)
    timeline_data_df = twitdata_dataframe(timeline_data)
    dbname = os.getenv('DB_NAME')
    search_colname = os.getenv('COL_NAME1')
    timeline_colname = os.getenv('COL_NAME2')
    writeIntoMongo(search_data_df, dbname, search_colname)
    writeIntoMongo(timeline_data_df, dbname, timeline_colname)
    searchdataframe = readFromMongo(dbname, search_colname)
    timelinedataframe = readFromMongo(dbname, timeline_colname)
    print(searchdataframe.show(5))
    print(timelinedataframe.show(5))
