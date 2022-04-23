import datetime
import time
import requests
from pymongo import MongoClient
from ratelimit import limits, sleep_and_retry

client = MongoClient('mongodb+srv://project_group_user:project_group_password_CSxx25@cluster0.qr33x.mongodb.net/CSxx25_Project_Database?retryWrites=true&w=majority')

@sleep_and_retry
@limits(calls=50, period=60)
def fetch_data_using_api(url, timeout):
    response_json = {}
    try:
        response = requests.get(url, timeout=timeout)
        response_json = response.json()
    except requests.Timeout as e:
        print('Exception occurred while fetching data from {} : {}'.format(url, e))
    finally:
        return response_json


def fetch_data_for_coin_and_insert_into_db(coin_id, num_of_days):
    db = client.get_database('coin_markets')
    db_sequences_collection = db.get_collection('sequences')

    json_data_usd = fetch_data_using_api('https://api.coingecko.com/api/v3/coins/{}/market_chart?vs_currency=usd&days={}&interval=daily'.format(coin_id, num_of_days), 10)

    last_id = db_sequences_collection.find_one({'_id': 'all_coins_data'}, projection={'seq': True, '_id': False})['seq']
    document_dict_list = []
    if 'prices' in json_data_usd:
        for index in range(len(json_data_usd['prices']) - 1):
            each_date_timestamp = int(json_data_usd['prices'][index][0] / 1000)
            each_date = datetime.datetime.utcfromtimestamp(each_date_timestamp).strftime('%Y-%m-%d')

            each_price_usd = json_data_usd['prices'][index][1]
            each_market_cap_usd = json_data_usd['market_caps'][index][1]
            each_total_volume_usd = json_data_usd['total_volumes'][index][1]

            last_id += 1
            each_document_dict = {'_id': last_id,
                                  'date': each_date,
                                  'utc_timestamp': each_date_timestamp,
                                  'coin_id': coin_id,
                                  'price_usd': each_price_usd,
                                  'market_cap_usd': each_market_cap_usd,
                                  'total_volume_usd': each_total_volume_usd}
            document_dict_list.append(each_document_dict)
            # print('{} inserted...'.format(each_document_dict))
    document_length = len(document_dict_list)
    if document_length > 0:
        db.get_collection('all_coins_data').insert_many(document_dict_list)
        db_sequences_collection.update_one({'_id': 'all_coins_data'}, {'$set': {'seq': last_id}})
    print('{} documents inserted into {} collection for {}...'.format(document_length, 'all_coins_data', coin_id))
    return document_length


def fetch_all_coins_static_and_insert_into_db():
    db = client.get_database('coin_markets')
    db_sequences_collection = db.get_collection('sequences')

    json_data = fetch_data_using_api('https://api.coingecko.com/api/v3/coins/list', 10)

    last_id = db_sequences_collection.find_one({'_id': 'all_coins_static'}, projection={'seq': True, '_id': False})['seq']
    document_dict_list = []
    for index in range(len(json_data)):
        last_id += 1
        each_document_dict = {'_id': last_id,
                              'coin_id': json_data[index]['id'],
                              'symbol': json_data[index]['symbol'],
                              'name': json_data[index]['name']}
        document_dict_list.append(each_document_dict)
    if len(document_dict_list) > 0:
        db.get_collection('all_coins_static').insert_many(document_dict_list)
        db_sequences_collection.update_one({'_id': 'all_coins_static'}, {'$set': {'seq': last_id}})
    print('{} documents inserted into {} collection...'.format(len(document_dict_list), 'all_coins_static'))


def fetch_historical_data_for_all_coins_and_insert_into_db():
    db = client.get_database('coin_markets')
    db_coins_static_collection = db.get_collection('all_coins_static')

    prev_document_length = 0
    for each_coin_id in db_coins_static_collection.distinct('coin_id'):
        print('========================================================================================')
        print('Start of loading historical data for {}...'.format(each_coin_id))
        current_document_length = fetch_data_for_coin_and_insert_into_db(each_coin_id, 'max')
        if prev_document_length == 0 and current_document_length == 0:
            time.sleep(10)
        prev_document_length = current_document_length
        print('End of loading historical data for {}...'.format(each_coin_id))
        print('========================================================================================')


def fetch_daily_data_for_all_coins_and_insert_into_db():
    db = client.get_database('coin_markets')
    db_coins_static_collection = db.get_collection('all_coins_static')

    prev_document_length = 0
    for each_coin_id in db_coins_static_collection.distinct('coin_id'):
        print('========================================================================================')
        print('Start of loading daily data for {}...'.format(each_coin_id))
        current_document_length = fetch_data_for_coin_and_insert_into_db(each_coin_id, '1')
        if prev_document_length == 0 and current_document_length == 0:
            time.sleep(10)
        prev_document_length = current_document_length
        print('End of loading daily data for {}...'.format(each_coin_id))
        print('========================================================================================')


## Run only once to fetch all static data related to all cryptocurrencies ##
fetch_all_coins_static_and_insert_into_db()

## Run only once to fetch historical data (price, market_cap, volume in usd) for all cryptocurrencies ##
fetch_historical_data_for_all_coins_and_insert_into_db()
