from __future__ import print_function

import json, boto3, ast, sys, base64

print('Loading function')

# Validating Email address if its not like nrsh13@gmail.com
def validate_email(email_address):
	try:
		import re
		match = re.match('^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,4})$', email_address)

		if match == None:
			status = False
		else:
			status = True
		return status
		
	except Exception,e:
		print ("Failed to Validate the Email Address !!")
		print ("ERROR: " , e)

		
		
def check_table_existence(tableName='users'):
        try:
				# Used for Listing Tables.
				client = boto3.client('dynamodb',
                                aws_access_key_id='yourAccessKeyID',
                                aws_secret_access_key='yourSecretAccessKey',
                                region_name='us-east-1')

				# Resource Object - Used for Creating Table
				dynamodb = boto3.resource('dynamodb',
                                aws_access_key_id='yourAccessKeyID',
                                aws_secret_access_key='yourSecretAccessKey',
                                region_name='us-east-1')

				result = client.list_tables()['TableNames']
				
				if tableName not in result:
					# Create the DynamoDB table.
					table = dynamodb.create_table(
                        TableName=tableName,
                        KeySchema=[
                        {
                            'AttributeName': 'email',
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': 'ipaddress',
                            'KeyType': 'RANGE'
                        }
                            ],

                        AttributeDefinitions=[
                        {
                            'AttributeName': 'email',
                            'AttributeType': 'S'
                        },
                        {
                            'AttributeName': 'ipaddress',
                            'AttributeType': 'S'
                        },
                            ],

                        StreamSpecification={
                                'StreamEnabled': True,
                                'StreamViewType': 'NEW_IMAGE'
                        },

                        ProvisionedThroughput={
                                'ReadCapacityUnits': 5,
                                'WriteCapacityUnits': 5
                        }
                        )


                    # Wait until the table exists.
					table.meta.client.get_waiter('table_exists').wait(TableName=tableName)
					
				else:
					print ("\nTable Exists")
					table = dynamodb.Table(tableName)
					
				print ("\nRunning a COUNT(*) Test")
				print ("\nCount(*) Result: " , (table.item_count))


        except Exception,e:
                print ("\nFailed to CREATE Table. Exiting with ERROR:")
                print ("\n   Exiting with ERROR:")
                print ("\n   " , e, "\n")
                sys.exit()


# Writing to DynamoDB Table 'users'. Adding an extra Column 'email_status' - Valid or Invalid.

def write_to_dynamodb(payload):
	try:
			client = boto3.resource('dynamodb',region_name='us-east-1')
			table = client.Table('users')
			
			payload = json.loads(payload)
			
			# Creating a New Column based on email address validity
			payload['email'] = payload['email'].lower()			
			status = validate_email(payload['email'])
			if status:
				payload['email_status'] = 'valid'	
			else:
				payload['email_status'] = 'invalid'
				
			table.put_item(Item=payload)
                
	except Exception,e:
		print ("Failed to Write to DynamoDB Table in Another Region due to below ERROR !!")
		print ("ERROR: " , e)


def lambda_handler(event, context):
    check_table_existence()
    #print("Received event: " + json.dumps(event, indent=2))
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record['kinesis']['data'])
        print("Decoded payload: " + payload)
        write_to_dynamodb(payload)
    return 'Successfully processed {} records.'.format(len(event['Records']))
