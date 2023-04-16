import boto3

# create a DynamoDB client object
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

query2()