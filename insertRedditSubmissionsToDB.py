import datetime
import requests
from pymongo import MongoClient

client = MongoClient('mongodb+srv://project_group_user:project_group_password_CSxx25@cluster0.qr33x.mongodb.net/CSxx25_Project_Database?retryWrites=true&w=majority')


def fetch_reddit_submissions_for_year_and_subreddit(year, subreddit):
    db = client.get_database('Reddit_Submissions')
    db_sequences_collection = db.get_collection('sequences')
    collection_name = 'Submissions_' + str(year)
    db_collection_to_insert = db.get_collection(collection_name)

    month_days = {'01': 31, '02': 28, '03': 31, '04': 30, '05': 31, '06': 30, '07': 31, '08': 31, '09': 30, '10': 31, '11': 30, '12': 31}
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    each_hour_increment = 60 * 60

    ts_after = int(datetime.datetime(year, 1, 1, 0, 0, 0).replace(tzinfo=datetime.timezone.utc).timestamp())
    for month in months:
        final_dict = []
        days = month_days[month]
        if year % 4 == 0 and month == '02':
            days += 1
        last_id = db_sequences_collection.find_one({'_id': collection_name}, projection={'seq': True, '_id': False})['seq']
        for day in range(days):
            for hour in range(24):
                ts_before = ts_after + each_hour_increment
                url = "https://api.pushshift.io/reddit/search/submission/?size=100&after={}&before={}&subreddit={}&fields=subreddit,title,score,num_comments,created_utc,selftext,subreddit_id".format(ts_after, ts_before, subreddit)
                try:
                    response = requests.get(url, timeout=10)
                    response_json = response.json()
                    final = response_json['data']
                    if len(final) != 0:
                        for i in range(0, len(final)):
                            val = final[i]
                            sub = val['subreddit']
                            title = val['title']
                            score = val['score']
                            created_utc = val['created_utc']
                            num_comments = val['num_comments']
                            each_date = datetime.datetime.utcfromtimestamp(created_utc).strftime('%Y-%m-%d')
                            selftext = val['selftext']
                            subreddit_id = val['subreddit_id']
                            last_id += 1
                            submissions_dict = {
                                "_id": last_id,
                                "subreddit": sub,
                                "title": title,
                                "score": score,
                                "num_comments": num_comments,
                                "created_utc": created_utc,
                                "date": each_date,
                                "selftext": selftext,
                                "subreddit_id": subreddit_id
                            }
                            final_dict.append(submissions_dict)
                except Exception as e:
                    print('Exception encountered {}'.format(e))
                ts_after = ts_before
            print('Completed for: {}'.format(str(year) + '-' + month + '-' + str(day + 1)))
        if len(final_dict) > 0:
            db_collection_to_insert.insert_many(final_dict)
            db_sequences_collection.update_one({'_id': collection_name}, {'$set': {'seq': last_id}})


for year in range(2014, 2023):
    print('Fetching submissions data for {}'.format(year))
    fetch_reddit_submissions_for_year_and_subreddit(year, 'Bitcoin')
