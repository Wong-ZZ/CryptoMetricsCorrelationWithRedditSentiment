from pymongo import MongoClient, UpdateOne
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from joblib import Parallel, delayed
from multiprocessing import Process

SUBMISSIONS = 'Submissions'
COMMENTS = 'Comments'

CONNECTION_STRING = 'mongodb+srv://project_group_user:project_group_password_CSxx25@cluster0.qr33x.mongodb.net/CSxx25_Project_Database?retryWrites=true&w=majority'
client = MongoClient(CONNECTION_STRING)

sid = SentimentIntensityAnalyzer()

def process_submission(doc):
    _id = doc['_id']
    title = doc['title']
    selftext = doc['selftext']
    sentiment_score = sid.polarity_scores(f'{title} {selftext}')['compound']
    return { '_id': _id, 'sentiment_score': sentiment_score }

def process_comment(doc):
    _id = doc['_id']
    sentiment_score = sid.polarity_scores(doc['body'])['compound']
    return { '_id': _id, 'sentiment_score': sentiment_score }

def uploadSentiment(data, year, collectionName):
    MongoClient(CONNECTION_STRING)[f'Reddit_{collectionName}'][f'{collectionName}_{year}'].bulk_write(
        [
            UpdateOne(
                {"_id": i['_id']},
                { '$set': { 'sentiment_score': i['sentiment_score'] } },
                upsert=True)
            for i in data
        ])
    print(f'uploaded {collectionName} sentiment for year {year}')

def processSentiment(collectionName):
    process_fn = {SUBMISSIONS: process_submission, COMMENTS: process_comment}[collectionName]
    db = client[f'Reddit_{collectionName}']
    processes = []
    for year in range(2014, 2023):
        collection = db[f'{collectionName}_{year}']
        print(f'computing {collectionName} sentiment for year {year}')
        data = Parallel(n_jobs=-1, verbose=0)(delayed(process_fn)(doc) for doc in collection.find())
        process = Process(target=lambda: uploadSentiment(data, year, collectionName))
        process.start()
        print(f'uploading {collectionName} sentiment for year {year}')

    for process in processes:
        process.join()

if __name__ == "__main__":
    processSentiment(SUBMISSIONS)
    processSentiment(COMMENTS)
