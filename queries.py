import boto3

# create a DynamoDB client object
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
	location = 'NOVA' # specify the location to search for

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

	# print the results
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

	# Print the top k users
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

	# Print the top k users
	for user in top_k_users:
		print(user['tweet_id'])

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
