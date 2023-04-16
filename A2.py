import boto3
import os
import csv

dynamodb = boto3.resource('dynamodb', region_name='us-west-2', aws_access_key_id='AKIAXFLZELRT5YARSFUC', aws_secret_access_key='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')

table_name = 'BitcoinTweet'

#table = dynamodb.Table('Bitcoin')



table = dynamodb.create_table(
    TableName=table_name,
    KeySchema=[
        {
            'AttributeName': 'user_id',
            'KeyType': 'HASH'  
        },
        {
            'AttributeName': 'tweet_id',
            'KeyType': 'RANGE'  
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'user_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'tweet_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'user_followers',
            'AttributeType': 'N'
        }
    ],
    GlobalSecondaryIndexes=[
        {
            'IndexName': 'user_followers_index',
            'KeySchema': [
                {
                    'AttributeName': 'user_followers',
                    'KeyType': 'HASH'
                }
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            },
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)

table.wait_until_exists()

print("Table created:", table.table_name)
with open('Bitcoin_Tweets.csv', 'r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    i = 0
    for row in reader:
        if row['user_name']:
            table.put_item(
                Item={
                    'user_id': row['user_name'],
                    'tweet_id': row['text'],
                    'user_location': row['user_location'],
                    'user_description': row['user_description'],
                    'user_created': row['user_created'],
                    'user_followers': int(float(row['user_followers'])),
                    'user_friends': int(row['user_friends']),
                    'user_favorites': int(row['user_favourites']),
                    'user_verified': row['user_verified'],
                    'date': row['date'],
                    'hashtags': row['hashtags'],
                    'source': row['source'],
                    'is_retweet': row['is_retweet']
                }
            )
            i += 1
            if i >= 1000:
                break

print("Data inserted into table:", table.table_name)