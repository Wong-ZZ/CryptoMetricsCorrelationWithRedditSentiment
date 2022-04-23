# price / sentiment score
# price / comment freq
# volume / sentiment score
# volume / comment freq
import numpy as np
from pymongo import MongoClient
from pyspark.sql import SparkSession
import json
from os.path import exists
from pyspark.sql.functions import col

SUBMISSIONS_DB = 'Reddit_Submissions'
SUBMISSIONS_COLLECTION = 'Submissions_{year}'
COMMENTS_DB = 'Reddit_Comments'
COMMENTS_COLLECTION = 'Comments_{year}'
CRYPTO_DB = 'coin_markets'
CRYPTO_COLLECTION = 'all_coins_data'


def establish_connection():
    CONNECTION_STRING = 'mongodb+srv://project_group_user:project_group_password_CSxx25@cluster0.qr33x.mongodb.net/CSxx25_Project_Database?retryWrites=true&w=majority'
    client = MongoClient(CONNECTION_STRING)
    return client


def query(client, dbName, collectionName):
    db = client[dbName]
    return db[collectionName]


def write(result, fileName):
    f = open(fileName, "a")
    for line in result:
        f.write(json.dumps(line) + '\n')
    f.close()


def pull_comment_data():
    client = establish_connection()
    print('Pulling Reddit Comment Data...')
    if exists('comments.json'):
        return
    for year in range(2014, 2023):
        fileName = 'comments.json'
        collection = query(client, COMMENTS_DB, COMMENTS_COLLECTION.format(year=year))
        write(collection.find(), fileName)


def pull_crypto_data():
    client = establish_connection()
    print('Pulling Bitcoin data...')
    fileName = 'crypto.json'
    if not exists(fileName):
        collection = query(client, CRYPTO_DB, CRYPTO_COLLECTION)
        write(collection.find(), fileName)


def daily_price_and_sentiment_score(cryptoDF, commentDF):
    daily_price_and_sentiment_score_DF = commentDF.groupBy('date').mean('sentiment_score').withColumnRenamed('avg(sentiment_score)', 'sentiment_score') \
            .join(cryptoDF, commentDF.date == cryptoDF.date, 'inner') \
            .select(commentDF.date, 'sentiment_score', 'price_usd') \
            .orderBy(col('date'))

    if not exists('Project_Results/days.npy'):
        days = np.array(convert_to_flat_list(daily_price_and_sentiment_score_DF.select('date').collect()))
        np.save('Project_Results/days.npy', days)

    if not exists('Project_Results/daily_sentiment_score.npy'):
        daily_sentiment_score = np.array(convert_to_flat_list(daily_price_and_sentiment_score_DF.select('sentiment_score').collect()))
        np.save('Project_Results/daily_sentiment_score.npy', daily_sentiment_score)

    if not exists('Project_Results/daily_bitcoin_price.npy'):
        daily_bitcoin_price = np.array(convert_to_flat_list(daily_price_and_sentiment_score_DF.select('price_usd').collect()))
        np.save('Project_Results/daily_bitcoin_price.npy', daily_bitcoin_price)


def daily_volume_and_sentiment_score(cryptoDF, commentDF):
    daily_volume_and_sentiment_score_DF = commentDF.groupBy('date').mean('sentiment_score').withColumnRenamed('avg(sentiment_score)', 'sentiment_score') \
            .join(cryptoDF, commentDF.date == cryptoDF.date, 'inner') \
            .select(commentDF.date, 'sentiment_score', 'total_volume_usd') \
            .orderBy(col('date'))

    if not exists('Project_Results/days.npy'):
        days = np.array(convert_to_flat_list(daily_volume_and_sentiment_score_DF.select('date').collect()))
        np.save('Project_Results/days.npy', days)

    if not exists('Project_Results/daily_sentiment_score.npy'):
        daily_sentiment_score = np.array(convert_to_flat_list(daily_volume_and_sentiment_score_DF.select('sentiment_score').collect()))
        np.save('Project_Results/daily_sentiment_score.npy', daily_sentiment_score)

    if not exists('Project_Results/daily_total_volume.npy'):
        daily_total_volume = np.array(convert_to_flat_list(daily_volume_and_sentiment_score_DF.select('total_volume_usd').collect()))
        np.save('Project_Results/daily_total_volume.npy', daily_total_volume)


def monthly_price_and_sentiment_score(cryptoDF, commentDF):
    commentDF = commentDF.withColumn('month', col('date').substr(1, 7)) \
                    .groupBy('month').mean('sentiment_score').withColumnRenamed('avg(sentiment_score)', 'sentiment_score')
    cryptoDF = cryptoDF.withColumn('month', col('date').substr(1, 7)) \
                .groupBy('month').mean('price_usd').withColumnRenamed('avg(price_usd)', 'price_usd')
    monthly_price_and_sentiment_score_DF = commentDF.join(cryptoDF, commentDF.month == cryptoDF.month, 'inner') \
            .select(commentDF.month, 'sentiment_score', 'price_usd') \
            .orderBy(col('month'))

    if not exists('Project_Results/months.npy'):
        months = np.array(convert_to_flat_list(monthly_price_and_sentiment_score_DF.select('month').collect()))
        np.save('Project_Results/months.npy', months)

    if not exists('Project_Results/monthly_sentiment_score.npy'):
        monthly_sentiment_score = np.array(convert_to_flat_list(monthly_price_and_sentiment_score_DF.select('sentiment_score').collect()))
        np.save('Project_Results/monthly_sentiment_score.npy', monthly_sentiment_score)

    if not exists('Project_Results/monthly_bitcoin_price.npy'):
        monthly_bitcoin_price = np.array(convert_to_flat_list(monthly_price_and_sentiment_score_DF.select('price_usd').collect()))
        np.save('Project_Results/monthly_bitcoin_price.npy', monthly_bitcoin_price)


def monthly_volume_and_sentiment_score(cryptoDF, commentDF):
    commentDF = commentDF.withColumn('month', col('date').substr(1, 7)) \
                    .groupBy('month').mean('sentiment_score').withColumnRenamed('avg(sentiment_score)', 'sentiment_score')
    cryptoDF = cryptoDF.withColumn('month', col('date').substr(1, 7)) \
                .groupBy('month').mean('total_volume_usd').withColumnRenamed('avg(total_volume_usd)', 'total_volume_usd')
    monthly_volume_and_sentiment_score_DF = commentDF.join(cryptoDF, commentDF.month == cryptoDF.month, 'inner') \
            .select(commentDF.month, 'sentiment_score', 'total_volume_usd') \
            .orderBy(col('month'))

    if not exists('Project_Results/months.npy'):
        months = np.array(convert_to_flat_list(monthly_volume_and_sentiment_score_DF.select('month').collect()))
        np.save('Project_Results/months.npy', months)

    if not exists('Project_Results/monthly_sentiment_score.npy'):
        monthly_sentiment_score = np.array(convert_to_flat_list(monthly_volume_and_sentiment_score_DF.select('sentiment_score').collect()))
        np.save('Project_Results/monthly_sentiment_score.npy', monthly_sentiment_score)

    if not exists('Project_Results/monthly_total_volume.npy'):
        monthly_total_volume = np.array(convert_to_flat_list(monthly_volume_and_sentiment_score_DF.select('total_volume_usd').collect()))
        np.save('Project_Results/monthly_total_volume.npy', monthly_total_volume)


def yearly_price_and_sentiment_score(cryptoDF, commentDF):
    commentDF = commentDF.withColumn('year', col('date').substr(1, 4)) \
                    .groupBy('year').mean('sentiment_score').withColumnRenamed('avg(sentiment_score)', 'sentiment_score')
    cryptoDF = cryptoDF.withColumn('year', col('date').substr(1, 4)) \
                .groupBy('year').mean('price_usd').withColumnRenamed('avg(price_usd)', 'price_usd')
    yearly_price_and_sentiment_score_DF = commentDF.join(cryptoDF, commentDF.year == cryptoDF.year, 'inner') \
            .select(commentDF.year, 'sentiment_score', 'price_usd') \
            .orderBy(col('year'))

    if not exists('Project_Results/years.npy'):
        years = np.array(convert_to_flat_list(yearly_price_and_sentiment_score_DF.select('year').collect()))
        np.save('Project_Results/years.npy', years)

    if not exists('Project_Results/yearly_sentiment_score.npy'):
        yearly_sentiment_score = np.array(convert_to_flat_list(yearly_price_and_sentiment_score_DF.select('sentiment_score').collect()))
        np.save('Project_Results/yearly_sentiment_score.npy', yearly_sentiment_score)

    if not exists('Project_Results/yearly_bitcoin_price.npy'):
        yearly_bitcoin_price = np.array(convert_to_flat_list(yearly_price_and_sentiment_score_DF.select('price_usd').collect()))
        np.save('Project_Results/yearly_bitcoin_price.npy', yearly_bitcoin_price)


def yearly_volume_and_sentiment_score(cryptoDF, commentDF):
    commentDF = commentDF.withColumn('year', col('date').substr(1, 4)) \
                    .groupBy('year').mean('sentiment_score').withColumnRenamed('avg(sentiment_score)', 'sentiment_score')
    cryptoDF = cryptoDF.withColumn('year', col('date').substr(1, 4)) \
                .groupBy('year').mean('total_volume_usd').withColumnRenamed('avg(total_volume_usd)', 'total_volume_usd')
    yearly_volume_and_sentiment_score_DF = commentDF.join(cryptoDF, commentDF.year == cryptoDF.year, 'inner') \
            .select(commentDF.year, 'sentiment_score', 'total_volume_usd') \
            .orderBy(col('year'))

    if not exists('Project_Results/years.npy'):
        years = np.array(convert_to_flat_list(yearly_volume_and_sentiment_score_DF.select('year').collect()))
        np.save('Project_Results/years.npy', years)

    if not exists('Project_Results/yearly_sentiment_score.npy'):
        yearly_sentiment_score = np.array(convert_to_flat_list(yearly_volume_and_sentiment_score_DF.select('sentiment_score').collect()))
        np.save('Project_Results/yearly_sentiment_score.npy', yearly_sentiment_score)

    if not exists('Project_Results/yearly_total_volume.npy'):
        yearly_total_volume = np.array(convert_to_flat_list(yearly_volume_and_sentiment_score_DF.select('total_volume_usd').collect()))
        np.save('Project_Results/yearly_total_volume.npy', yearly_total_volume)


def daily_price_and_frequency(cryptoDF, commentDF):
    daily_price_and_frequency_DF = commentDF.groupBy('date').count().withColumnRenamed('count', 'frequency') \
            .join(cryptoDF, commentDF.date == cryptoDF.date, 'inner') \
            .select(commentDF.date, 'price_usd', 'frequency') \
            .orderBy(col('date'))

    if not exists('Project_Results/days.npy'):
        days = np.array(convert_to_flat_list(daily_price_and_frequency_DF.select('date').collect()))
        np.save('Project_Results/days.npy', days)

    if not exists('Project_Results/daily_comments_frequency.npy'):
        daily_comments_frequency = np.array(convert_to_flat_list(daily_price_and_frequency_DF.select('frequency').collect()))
        np.save('Project_Results/daily_comments_frequency.npy', daily_comments_frequency)

    if not exists('Project_Results/daily_bitcoin_price.npy'):
        daily_bitcoin_price = np.array(convert_to_flat_list(daily_price_and_frequency_DF.select('price_usd').collect()))
        np.save('Project_Results/daily_bitcoin_price.npy', daily_bitcoin_price)


def daily_volume_and_frequency(cryptoDF, commentDF):
    daily_volume_and_frequency_DF = commentDF.groupBy('date').count().withColumnRenamed('count', 'frequency') \
            .join(cryptoDF, commentDF.date == cryptoDF.date, 'inner') \
            .select(commentDF.date, 'total_volume_usd', 'frequency') \
            .orderBy(col('date'))

    if not exists('Project_Results/days.npy'):
        days = np.array(convert_to_flat_list(daily_volume_and_frequency_DF.select('date').collect()))
        np.save('Project_Results/days.npy', days)

    if not exists('Project_Results/daily_comments_frequency.npy'):
        daily_comments_frequency = np.array(convert_to_flat_list(daily_volume_and_frequency_DF.select('frequency').collect()))
        np.save('Project_Results/daily_comments_frequency.npy', daily_comments_frequency)

    if not exists('Project_Results/daily_total_volume.npy'):
        daily_total_volume = np.array(convert_to_flat_list(daily_volume_and_frequency_DF.select('total_volume_usd').collect()))
        np.save('Project_Results/daily_total_volume.npy', daily_total_volume)


def monthly_price_and_frequency(cryptoDF, commentDF):
    commentDF = commentDF.withColumn('month', col('date').substr(1, 7)) \
                    .groupBy('month').count().withColumnRenamed('count', 'frequency')
    cryptoDF = cryptoDF.withColumn('month', col('date').substr(1, 7)) \
                .groupBy('month').mean('price_usd').withColumnRenamed('avg(price_usd)', 'price_usd')
    monthly_price_and_frequency_DF = commentDF.join(cryptoDF, commentDF.month == cryptoDF.month, 'inner') \
            .select(commentDF.month, 'frequency', 'price_usd') \
            .orderBy('month')

    if not exists('Project_Results/months.npy'):
        months = np.array(convert_to_flat_list(monthly_price_and_frequency_DF.select('month').collect()))
        np.save('Project_Results/months.npy', months)

    if not exists('Project_Results/monthly_comments_frequency.npy'):
        monthly_comments_frequency = np.array(convert_to_flat_list(monthly_price_and_frequency_DF.select('frequency').collect()))
        np.save('Project_Results/monthly_comments_frequency.npy', monthly_comments_frequency)

    if not exists('Project_Results/monthly_bitcoin_price.npy'):
        monthly_bitcoin_price = np.array(convert_to_flat_list(monthly_price_and_frequency_DF.select('price_usd').collect()))
        np.save('Project_Results/monthly_bitcoin_price.npy', monthly_bitcoin_price)


def monthly_volume_and_frequency(cryptoDF, commentDF):
    commentDF = commentDF.withColumn('month', col('date').substr(1, 7)) \
                    .groupBy('month').count().withColumnRenamed('count', 'frequency')
    cryptoDF = cryptoDF.withColumn('month', col('date').substr(1, 7)) \
                .groupBy('month').mean('total_volume_usd').withColumnRenamed('avg(total_volume_usd)', 'total_volume_usd')
    monthly_volume_and_frequency_DF = commentDF.join(cryptoDF, commentDF.month == cryptoDF.month, 'inner') \
            .select(commentDF.month, 'frequency', 'total_volume_usd') \
            .orderBy(col('month'))

    if not exists('Project_Results/months.npy'):
        months = np.array(convert_to_flat_list(monthly_volume_and_frequency_DF.select('month').collect()))
        np.save('Project_Results/months.npy', months)

    if not exists('Project_Results/monthly_comments_frequency.npy'):
        monthly_comments_frequency = np.array(convert_to_flat_list(monthly_volume_and_frequency_DF.select('frequency').collect()))
        np.save('Project_Results/monthly_comments_frequency.npy', monthly_comments_frequency)

    if not exists('Project_Results/monthly_total_volume.npy'):
        monthly_total_volume = np.array(convert_to_flat_list(monthly_volume_and_frequency_DF.select('total_volume_usd').collect()))
        np.save('Project_Results/monthly_total_volume.npy', monthly_total_volume)


def yearly_price_and_frequency(cryptoDF, commentDF):
    commentDF = commentDF.withColumn('year', col('date').substr(1, 4)) \
                    .groupBy('year').count().withColumnRenamed('count', 'frequency')
    cryptoDF = cryptoDF.withColumn('year', col('date').substr(1, 4)) \
                .groupBy('year').mean('price_usd').withColumnRenamed('avg(price_usd)', 'price_usd')
    yearly_price_and_frequency_DF = commentDF.join(cryptoDF, commentDF.year == cryptoDF.year, 'inner') \
            .select(commentDF.year, 'frequency', 'price_usd') \
            .orderBy(col('year'))

    if not exists('Project_Results/years.npy'):
        years = np.array(convert_to_flat_list(yearly_price_and_frequency_DF.select('year').collect()))
        np.save('Project_Results/years.npy', years)

    if not exists('Project_Results/yearly_comments_frequency.npy'):
        yearly_comments_frequency = np.array(convert_to_flat_list(yearly_price_and_frequency_DF.select('frequency').collect()))
        np.save('Project_Results/yearly_comments_frequency.npy', yearly_comments_frequency)

    if not exists('Project_Results/yearly_bitcoin_price.npy'):
        yearly_bitcoin_price = np.array(convert_to_flat_list(yearly_price_and_frequency_DF.select('price_usd').collect()))
        np.save('Project_Results/yearly_bitcoin_price.npy', yearly_bitcoin_price)


def yearly_volume_and_frequency(cryptoDF, commentDF):
    commentDF = commentDF.withColumn('year', col('date').substr(1, 4)) \
                    .groupBy('year').count().withColumnRenamed('count', 'frequency')
    cryptoDF = cryptoDF.withColumn('year', col('date').substr(1, 4)) \
                .groupBy('year').mean('total_volume_usd').withColumnRenamed('avg(total_volume_usd)', 'total_volume_usd')
    yearly_volume_and_frequency_DF = commentDF.join(cryptoDF, commentDF.year == cryptoDF.year, 'inner') \
            .select(commentDF.year, 'frequency', 'total_volume_usd') \
            .orderBy(col('year'))

    if not exists('Project_Results/years.npy'):
        years = np.array(convert_to_flat_list(yearly_volume_and_frequency_DF.select('year').collect()))
        np.save('Project_Results/years.npy', years)

    if not exists('Project_Results/yearly_comments_frequency.npy'):
        yearly_comments_frequency = np.array(convert_to_flat_list(yearly_volume_and_frequency_DF.select('frequency').collect()))
        np.save('Project_Results/yearly_comments_frequency.npy', yearly_comments_frequency)

    if not exists('Project_Results/yearly_total_volume.npy'):
        yearly_total_volume = np.array(convert_to_flat_list(yearly_volume_and_frequency_DF.select('total_volume_usd').collect()))
        np.save('Project_Results/yearly_total_volume.npy', yearly_total_volume)


def convert_to_flat_list(initial_list):
    flat_list_to_return = []
    for sublist in initial_list:
        for item in sublist:
            flat_list_to_return.append(item)

    return flat_list_to_return


def main():
    spark = SparkSession.builder.master('local[*]').config('spark.driver.memory', '15g').appName('app').getOrCreate()
    cryptoDF = spark.read.json('crypto.json')
    cryptoDF = cryptoDF.where(cryptoDF.coin_id == 'bitcoin').cache()
    commentDF = spark.read.json('comments.json').cache()

    commentDF.printSchema()
    cryptoDF.printSchema()

    yearly_price_and_sentiment_score(cryptoDF, commentDF)
    yearly_volume_and_sentiment_score(cryptoDF, commentDF)
    yearly_price_and_frequency(cryptoDF, commentDF)
    yearly_volume_and_frequency(cryptoDF, commentDF)

    monthly_price_and_sentiment_score(cryptoDF, commentDF)
    monthly_volume_and_sentiment_score(cryptoDF, commentDF)
    monthly_price_and_frequency(cryptoDF, commentDF)
    monthly_price_and_frequency(cryptoDF, commentDF)

    daily_price_and_sentiment_score(cryptoDF, commentDF)
    daily_volume_and_sentiment_score(cryptoDF, commentDF)
    daily_price_and_frequency(cryptoDF, commentDF)
    daily_price_and_frequency(cryptoDF, commentDF)


pull_comment_data()
pull_crypto_data()
main()
