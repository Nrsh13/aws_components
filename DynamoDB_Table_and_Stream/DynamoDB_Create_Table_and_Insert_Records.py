import boto3, sys, json

import socket, json, sys, time, random, signal
import os, random, argparse
import datetime
from dateutil.relativedelta import relativedelta
import struct, socket


def usage():
        print "\n       python "+sys.argv[0]+" TableName\n"
        sys.exit()


def signal_handler(signal, frame):
    print("\n\nCTRL+c  Pressed. Program Exiting Gracefully !!\n")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


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



def produce_messages(client,table_obj,tableName):
        """ Produce Messages to the Given Stream """

        fnames = ["James","John","Robert","Michael","William","David","Richard","Joseph","Thomas","Charles","Christopher","Daniel","Matthew","Anthony","Donald","Mark","Paul","Steven","Andrew","Kenneth","George","Joshua","Kevin","Brian","Edward","Ronald","Timothy","Jason","Jeffrey","Ryan","Gary","Jacob","Nicholas","Eric","Stephen","Jonathan","Larry","Justin","Scott","Frank","Brandon","Raymond","Gregory","Benjamin","Samuel","Patrick","Alexander","Jack","Dennis","Jerry","Tyler","Aaron","Henry","Douglas","Jose","Peter","Adam","Zachary","Nathan","Walter","Harold","Kyle","Carl","Arthur","Gerald","Roger","Keith","Jeremy","Terry","Lawrence","Sean","Christian","Albert","Joe","Ethan","Austin","Jesse","Willie","Billy","Bryan","Bruce","Jordan","Ralph","Roy","Noah","Dylan","Eugene","Wayne","Alan","Juan","Louis","Russell","Gabriel","Randy","Philip","Harry","Vincent","Bobby","Johnny","Logan","naresh","ravi","Bhanu","akash","jane","gaurav","sailesh","tom","Andrea","Steve","Kris","Virender","Jason","stephen","Daemon","Elena","manu","nimisha","Bruce","michael","Akshay"]

        lnames = ["mith","ohnson","illiams","ones","rown","avis","iller","ilson","oore","Taylor","Anderson","Thomas","Jackson","White","Harris","Martin","Thompson","Garcia","Martinez","Robinson","Clark","Rodriguez","Lewis","Lee","Walker","Hall","Allen","Young","Hernandez","King","Wright","Lopez","Hill","Scott","Green","Adams","Baker","Gonzalez","Nelson","Carter","Mitchell","Perez","Roberts","Turner","Phillips","Campbell","Parker","Evans","Edwards","Collins","Stewart","Sanchez","Morris","Rogers","Reed","Cook","Morgan","Bell","Murphy","Bailey","Rivera","Cooper","Richardson","Cox","Howard","Ward","Torres","Peterson","Gray","Ramirez","James","Watson","Brooks","Kelly","Sanders","Price","Bennett","Wood","Barnes","Ross","Henderson","Coleman","Jenkins","Perry","Powell","Long","jangra","verma","sharma","weign","nain","devgun","gaundar","dagarin","thomas","ramayna","bourne","salvator","gilbert","beniwal","kumar","khanna","khaneja","singh","bansal","gupta","kaushik"]

        emails = ["@gmail.com","@@yahoo.com","@hotmail.com","@aol.com","@hotmail.co.uk","@rediffmail.com","@ymail.com","@outlook.com","hotmail.com","@hotmail.com","@bnz.co.nz","@nbc.com","@yahoo.com","#gmail.com","@hcl.com","@#tcs.com","@tcs.com","##hcl.com"]

        try:
            print "\nProducing N Messages sys.maxint. Press CTRL+C to STOP \n"
            for val in xrange(0,sys.maxint):
                fname=random.choice(fnames)
                lname=random.choice(lnames)
                email=random.choice(emails)
                ipaddress = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
                mobile = random.randint(9800000000,9899999999)
                passport_expiry_date = (datetime.datetime.now() + datetime.timedelta(random.randint(1,100)*365/12))
                passport_make_date = (passport_expiry_date - relativedelta(years=10))

                if val%5 == 0:
                        mobile = "9"+str(mobile)

                payload={"fname" : fname,"lname" : lname,"email" : fname.lower()+"_"+lname.lower()+email,"principal" : fname+"@EXAMPLE.COM","passport_make_date" : passport_make_date.strftime("%Y-%m-%d %H:%M:%S"),"passport_expiry_date" : passport_expiry_date.strftime("%Y-%m-%d %H:%M:%S"),"ipaddress" : ipaddress , "mobile" : mobile}

                print "Sending: " , payload , "\n"

                put_response = table_obj.put_item(
                        Item = payload
                        )

                time.sleep(2)

                if put_response['ResponseMetadata']['HTTPStatusCode'] == 200:
                        print "Payload Sent Successfully !!\n"
                else:
                        print "\nSomething WRONG with the Response received. HTTP Status Code: %s !!" %(put_response['ResponseMetadata']['HTTPStatusCode'])

        except Exception,e:
                print "\nFailed to Send Records to Given Stream !!"
                print "\n       Exiting with ERROR:"
                print "\n       " , e , "\n"
                sys.exit()



if __name__ == "__main__":

        # Check Number of Arguments
        if  len(sys.argv) != 2:
                print "\nTable Name Missing. Pass as an Argument. Exiting !!"
                usage()
        elif len(sys.argv) == 2 and sys.argv[1].lower() in ['h', 'help', 'usage']:
                usage()

        tableName =  sys.argv[1]

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

		# Used for PUT/DELETE Items
        table_obj = dynamodb.Table(tableName)

        print "\nChecking Table Existence"
        check_stream_existence(client,dynamodb,tableName)

        print "\nGenerate the Payload for Given Stream"
        produce_messages(client,table_obj,tableName)

