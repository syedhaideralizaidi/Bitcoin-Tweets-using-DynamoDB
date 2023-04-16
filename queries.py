import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb', region_name='us-west-2', aws_access_key_id='AKIAXFLZELRT5YARSFUC', aws_secret_access_key='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')

table = dynamodb.Table('BitcoinTweet')


def query1():
	response = table.query(
	    KeyConditionExpression='user_id = :val',
	    ExpressionAttributeValues={
	        ':val': 'CryptoND'
	    }
	)
	for item in response['Items']:
	    print(item['tweet_id'])

def query2():
	location = 'NOVA' 

	response = table.scan(
	    FilterExpression='user_location = :val',
	    ExpressionAttributeValues={
	        ':val': location
	    }
	)

	tweets = response['Items']

	while 'LastEvaluatedKey' in response:
	    response = table.scan(
	        FilterExpression='user_location = :val',
	        ExpressionAttributeValues={
	            ':val': location
	        },
	        ExclusiveStartKey=response['LastEvaluatedKey']
	    )
	    tweets.extend(response['Items'])

	
	for tweet in tweets:
	    print(tweet)

def query3():
	threshold = 1
	response = table.scan(
		FilterExpression='user_followers > :val',
		ExpressionAttributeValues={
			':val': threshold
		}
	)
	sorted_items = sorted(response['Items'], key=lambda x: x['user_followers'], reverse=True)
	k = 10
	top_k_users = sorted_items[:k]


	for user in top_k_users:
		print(user['user_id'])


def query4():
	threshold = 1
	response = table.scan(
		FilterExpression='user_followers > :val',
		ExpressionAttributeValues={
			':val': threshold
		}
	)
	sorted_items = sorted(response['Items'], key=lambda x: x['user_followers'], reverse=True)
	k = 10
	top_k_users = sorted_items[:k]

	for user in top_k_users:
		print(user['tweet_id'])

def query5():
	desired_hashtags = {'bitcoin', 'crypto', 'blockchain'}
	response = dynamodb.scan(
    TableName=table_name,
    FilterExpression='contains(hashtags, :h1) or contains(hashtags, :h2) or contains(hashtags, :h3)',
    ExpressionAttributeValues={
        ':h1': {'S': 'bitcoin'},
        ':h2': {'S': 'crypto'},
        ':h3': {'S': 'blockchain'}
    	}
	)

	tweet_count = {}
	for item in response['Items']:
	    tweet_id = item['tweet_id']['S']
	    if tweet_id in tweet_count:
	        tweet_count[tweet_id] += 1
	    else:
	        tweet_count[tweet_id] = 1

	top_k_tweets = sorted(tweet_count.items(), key=lambda x: x[1], reverse=True)[:k]

	for tweet_id, count in top_k_tweets:
	    print(f'Tweet ID: {tweet_id}, Frequency: {count}')		

def query6():
	threshold = 2
	response = table.scan(
		FilterExpression='user_followers < :val',
		ExpressionAttributeValues={
			':val': threshold
		}
	)
	for item in response['Items']:
		print("Deleting tweets for user with followers : ", item['user_followers'])

query6()
