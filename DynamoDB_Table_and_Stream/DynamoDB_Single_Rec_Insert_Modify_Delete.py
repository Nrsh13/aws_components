import boto3, sys, json


def usage():
        print "\n       python "+sys.argv[0]+" TableName\n"
        sys.exit()


def check_stream_existence(client,dynamodb,tableName):

        try:
                result = client.list_tables()['TableNames']

                if tableName not in result:
                    print "\nTable Does NOT Exist. Creating Now (Takes ~20 Secs)"

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
                        print "\nTable Exists"
                        table = dynamodb.Table(tableName)

                print "\nRunning a COUNT(*) Test"

                print "\nCount(*) Result: " , (table.item_count)

        except Exception,e:
                print "\nFailed to CREATE Table. Exiting with ERROR:"
                print "\n   Exiting with ERROR:"
                print "\n   " , e, "\n"
                sys.exit()




def insert_modify_delete(client,table_obj,tableName):

        print "\n==> Table was Created on - %s" %table_obj.creation_date_time

        print "\n==> Putting an ITEM"
        table_obj.put_item(
        Item={
                        'email': 'jane_doe@gmail.com',
                        'fname': 'Jane',
                        'lname': 'Doe',
                        'mobile': 9898756464,
                        'ipaddress': '192.13.24.266',
                }
        )


        print "\n==> Checking the New ITEM"
        response = table_obj.get_item(
                Key={
                        'email': 'jane_doe@gmail.com',
                        'ipaddress': '192.13.24.266'
                }
        )

        item = response['Item']
        print "\n==> Result is: " , item

        print "\n==> Updating ITEM by changing Mobile Number"
        table_obj.update_item(
                Key={
                        'email': 'jane_doe@gmail.com',
                        'ipaddress': '192.13.24.266'
                },
                UpdateExpression='SET mobile = :val1',
                ExpressionAttributeValues={
                        ':val1': 9992131312
                }
        )

        print "\n==> Checking the Updated ITEM"
        response = table_obj.get_item(
                Key={
                        'email': 'jane_doe@gmail.com',
                        'ipaddress': '192.13.24.266'
                }
        )
        item = response['Item']
        print "\n==> Result is: ", item


        print "\n==> DELETING an ITEM\n"
        table_obj.delete_item(
                Key={
                        'email': 'jane_doe@gmail.com',
                        'ipaddress': '192.13.24.266'
                }
        )


if __name__ == "__main__":

        # Check Number of Arguments
        if  len(sys.argv) != 2:
                print "\nTable Name Missing. Pass as an Argument. Exiting !!"
                usage()
        elif len(sys.argv) == 2 and sys.argv[1].lower() in ['h', 'help', 'usage']:
                usage()

        tableName =  sys.argv[1]

        client = boto3.client('dynamodb',
                                aws_access_key_id='yourAccessKeyID',
                                aws_secret_access_key='yourSecretAccessKey',
                                region_name='us-east-1')

        # Resource Object
        dynamodb = boto3.resource('dynamodb',
                                aws_access_key_id='yourAccessKeyID',
                                aws_secret_access_key='yourSecretAccessKey',
                                region_name='us-east-1')

        table_obj = dynamodb.Table(tableName)

        print "\nChecking Table Existence"
        check_stream_existence(client,dynamodb,tableName)

        print "\nGenerate the Payload for Given Stream"
        insert_modify_delete(client,table_obj,tableName)
