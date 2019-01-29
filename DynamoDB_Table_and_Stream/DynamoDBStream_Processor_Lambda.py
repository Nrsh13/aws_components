from __future__ import print_function

import json, boto3, ast, sys

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
                                region_name='ap-southeast-2')

				# Resource Object - Used for Creating Table
				dynamodb = boto3.resource('dynamodb',
                                aws_access_key_id='yourAccessKeyID',
                                aws_secret_access_key='yourSecretAccessKey',
                                region_name='ap-southeast-2')

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

				
# Writing Data to DynamoDB Table in Sydney Region. Any data being written to us-east-1 Region will be taken as INPUT and Replicated in Sydney Region.

def write_to_dynamodb(eventname,data):
	try:
			client = boto3.resource('dynamodb',region_name='ap-southeast-2')
			table = client.Table('users')
			event = json.loads(data)
			print (event)	
			
			# Collecting Record KEY (in case we got REMOVE event).
			event_key_details = event['Keys']
			key_col_names = event_key_details.keys()
			key_values = []
			for i in event_key_details.values():
				key_values.append(i.values()[0])
			key_to_delete = dict(zip(key_col_names,key_values))

			if eventname !=  'REMOVE':
				event_new_img = event['NewImage']
				# Create Record in proper format by removing the Data Type. Original record contains the Data Type also.
				# Org record - {"mobile": {"S": "99810889452"}, "lname": {"S": "Nelson"}
				col_names = event_new_img.keys()
				values = []
				for i in event_new_img.values():
					values.append(i.values()[0])
				complete_record = dict(zip(col_names,values))
				complete_record['email'] = complete_record['email'].lower()			
				status = validate_email(complete_record['email'])
				if status:
					complete_record['email_status'] = 'valid'	
				else:
					complete_record['email_status'] = 'invalid'
					
				# complete_record - {"mobile": "99810889452", "lname": "Nelson"} ==> NO Data Types included.
				table.put_item(Item=complete_record)
		
			else:
				
				# Remove the DELETED Item from Second REgion.
				table.delete_item(
                Key=key_to_delete
                )
                
	except Exception,e:
		print ("Failed to Write to DynamoDB Table in Another Region due to below ERROR !!")
		print ("ERROR: " , e)
		print ("printing event " , event)
  

# Taking Stream INPUT and Sending to S3 and DynamoDB Functions for Processing.
output = []
def lambda_handler(event, context):
	check_table_existence()
	for record in event['Records']:
		#Will use record['eventName'],record['eventName'] and record['dynamodb'] fields only.
		#print("DynamoDB Record: " + json.dumps(record['dynamodb'], indent=2))
		write_to_dynamodb(record['eventName'], json.dumps(record['dynamodb']))
		record['dynamodb']['eventName']=record['eventName']
        output.append(json.dumps(record['dynamodb']))
        
	return 'Successfully processed {} records.'.format(len(event['Records']))
