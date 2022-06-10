from tkinter import OFF
import mysql.connector
import pyspark
import tweepy
import os
from dotenv import load_dotenv
import sys
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import findspark
findspark.init()
findspark.add_packages('mysql:mysql-connector-java:8.0.11')


# Athentication Function
def get_twitter_auth():
    """
    @return:
        - the authentification to Twitter
    """
    try:
        api_key = os.getenv('API_KEY')
        api_key_secret = os.getenv('API_KEY_SECRET')
        access_token = os.getenv('ACCESS_TOKEN')
        access_token_secret = os.getenv('ACCESS_TOKEN_SECRET')
        consumer_key = api_key
        consumer_secret = api_key_secret
        access_token = access_token
        access_secret = access_token_secret

    except KeyError:
        sys.stderr.write("Twitter Environment Variable not Set\n")
        sys.exit(1)

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    return auth


# Client function to access the authentication API
def get_twitter_client():
    """
    @return:
        - the client to access the authentification API
    """
    auth = get_twitter_auth()
    client = tweepy.API(auth, wait_on_rate_limit=True)
    return client


# Function creating final dataframe
def get_tweets_from_user(clients, spark, twitter_user_name, page_limit=10, count_tweet=200):
    """
    @params:
        - twitter_user_name: the twitter username of a user (company, etc.)
        - page_limit: the total number of pages (max=16)
        - count_tweet: maximum number to be retrieved from a page

    @return
        - all the tweets from the user twitter_user_name
    """

    all_tweets = []

    for page in tweepy.Cursor(clients.user_timeline,
                              screen_name=twitter_user_name,
                              count=count_tweet).pages(page_limit):
        for tweet in page:
            parsed_tweet = {}
            parsed_tweet['id'] = tweet.id
            parsed_tweet['date'] = tweet.created_at.date()
            parsed_tweet['author'] = tweet.user.name
            parsed_tweet['twitter_name'] = tweet.user.screen_name
            parsed_tweet['text'] = tweet.text
            parsed_tweet['number_of_likes'] = tweet.favorite_count
            parsed_tweet['number_of_retweets'] = tweet.retweet_count
            all_tweets.append(parsed_tweet)

    # Create dataframe
    df = spark.createDataFrame(all_tweets)

    # Revome duplicates if there are any
    df = df.dropDuplicates()

    return df


# Save Function
def saveIntoCSV(dataframe):
    dataframe.coalesce(1).write.mode('overwrite').option("header", True).csv(
        'E:\TwitterDataAnalysis\ElevatedTwitData\extracted_elev_data')


def saveIntoMySQL(dataframe, jdbcUrl, table, user, password):
    dataframe.select("id", "date", "author", "twitter_name", "text", "number_of_likes", "number_of_retweets").write.format("jdbc") \
        .mode("overwrite") \
        .option('driver', "com.mysql.cj.jdbc.Driver")\
        .option("url", jdbcUrl) \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password", password) \
        .save()


# Write data into MongoDB
def writeIntoMongo(df):
    df.write.format("com.mongodb.spark.sql.DefaultSource")\
        .option("spark.mongodb.output.uri",
                "mongodb://localhost:27017/local.TwitData")\
        .mode("overwrite") \
        .save()


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("TwitData") \
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017/") \
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017/") \
        .config("spark.mongodb.output.database", "local")\
        .config("spark.driver.extraClassPath", "E:\spark\spark-3.1.3-bin-hadoop3.2\jars\mongo-spark-connector_2.12-3.0.1.jar") \
        .getOrCreate()
    load_dotenv('./ElevatedTwitData/credentials.env')
    client = get_twitter_client()
    googleAI = get_tweets_from_user(client, spark, "googleAI")
    print(pyspark.__version__)
    print(googleAI.show(20))
    saveIntoCSV(googleAI)
    database = os.getenv('DATABASE')
    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    table = os.getenv('TABLE')
    jdbcUrl = f"jdbc:mysql://localhost:3310/{database}?useSSL=false"
    jdbcDriver = "com.mysql.cj.jdbc.Driver"
    # saveIntoMySQL(googleAI, jdbcUrl, table, user, password)
    writeIntoMongo(googleAI)


# conf = pyspark.SparkConf().set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").setMaster(
    #     "local").setAppName("TwitData").setAll([('spark.driver.memory', '40g'), ('spark.executor.memory', '50g')])
    # sc = SparkContext(conf=conf)
    # mongo_ip = "mongodb://localhost:27017/local"
