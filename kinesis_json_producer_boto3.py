import socket, json, sys, time, random, signal
import os, random, argparse
import datetime
from dateutil.relativedelta import relativedelta
import struct, socket

import boto3


def usage():
    print """
    # Firehose: The Delivery Stream should exist. This Script will Produce Messages to the given Deilvery Stream.
    # Kinesis Stream: This Script will CREATE a STREAM (if Doesn't Exist) and Produce Messages:

             Usage: python %s 'firehose|kinesis' 'DeliveryStreamName|kStreamName'\n""" %(sys.argv[0].split('/')[-1])

    sys.exit()


def signal_handler(signal, frame):
    print("\n\nCTRL+c  Pressed. Program Exiting Gracefully !!\n")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


def check_stream_existence(service,client,streamName):
        try:
            if service == 'kinesis':

                result = client.list_streams()['StreamNames']
                if streamName not in result:
                    print "\nStream Does NOT Exist. Creating Now (Takes ~20 Secs)"
                    client.create_stream(StreamName=streamName,ShardCount=1)
                    time.sleep(20)
                    print "\nStream '%s' Created Successfully !!" %streamName
                else:
                    print "\nKinesis Stream '%s' Exists !!" %streamName

            else:

                result = client.list_delivery_streams( Limit=10, DeliveryStreamType='DirectPut')['DeliveryStreamNames']
                if streamName not in result:
                    print "\n   Delivery Stream '%s' Does NOT Exist. CREATE via CONSOLE and RERUN the Script." %streamName
                    print "\n   Existing !!\n"
                    sys.exit()
                else:
                    print "\nFirehose Delivery Stream '%s' Exists !!" %streamName

        except Exception,e:
            print "\nFailed to Fetch Stream Information !!"
            print "\n   Exiting with ERROR:"
            print "\n   " , e, "\n"
            sys.exit()


def produce_messages(service, client, streamName):
        """ Produce Messages to the Given Stream """

        fnames = ["James","John","Robert","Michael","William","David","Richard","Joseph","Thomas","Charles","Christopher","Daniel","Matthew","Anthony","Donald","Mark","Paul","Steven","Andrew","Kenneth","George","Joshua","Kevin","Brian","Edward","Ronald","Timothy","Jason","Jeffrey","Ryan","Gary","Jacob","Nicholas","Eric","Stephen","Jonathan","Larry","Justin","Scott","Frank","Brandon","Raymond","Gregory","Benjamin","Samuel","Patrick","Alexander","Jack","Dennis","Jerry","Tyler","Aaron","Henry","Douglas","Jose","Peter","Adam","Zachary","Nathan","Walter","Harold","Kyle","Carl","Arthur","Gerald","Roger","Keith","Jeremy","Terry","Lawrence","Sean","Christian","Albert","Joe","Ethan","Austin","Jesse","Willie","Billy","Bryan","Bruce","Jordan","Ralph","Roy","Noah","Dylan","Eugene","Wayne","Alan","Juan","Louis","Russell","Gabriel","Randy","Philip","Harry","Vincent","Bobby","Johnny","Logan","naresh","ravi","Bhanu","akash","jane","gaurav","sailesh","tom","Andrea","Steve","Kris","Virender","Jason","stephen","Daemon","Elena","manu","nimisha","Bruce","michael","Akshay"]

        lnames = ["mith","ohnson","illiams","ones","rown","avis","iller","ilson","oore","Taylor","Anderson","Thomas","Jackson","White","Harris","Martin","Thompson","Garcia","Martinez","Robinson","Clark","Rodriguez","Lewis","Lee","Walker","Hall","Allen","Young","Hernandez","King","Wright","Lopez","Hill","Scott","Green","Adams","Baker","Gonzalez","Nelson","Carter","Mitchell","Perez","Roberts","Turner","Phillips","Campbell","Parker","Evans","Edwards","Collins","Stewart","Sanchez","Morris","Rogers","Reed","Cook","Morgan","Bell","Murphy","Bailey","Rivera","Cooper","Richardson","Cox","Howard","Ward","Torres","Peterson","Gray","Ramirez","James","Watson","Brooks","Kelly","Sanders","Price","Bennett","Wood","Barnes","Ross","Henderson","Coleman","Jenkins","Perry","Powell","Long","jangra","verma","sharma","weign","nain","devgun","gaundar","dagarin","thomas","ramayna","bourne","salvator","gilbert","beniwal","kumar","khanna","khaneja","singh","bansal","gupta","kaushik"]

        emails = ["@gmail.com","@@yahoo.com","@hotmail.com","@aol.com","@hotmail.co.uk","@rediffmail.com","@ymail.com","@outlook.com","@@gmail.com","hotmail.com","@hotmail.com","@bnz.co.nz","@nbc.com","@yahoo.com","#gmail.com","@hcl.com","@#tcs.com","@tcs.com","##hcl.com"]

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

                payload={"fname" : fname,"lname" : lname,"email" : fname+"_"+lname+email,"principal" : fname+"@EXAMPLE.COM","passport_make_date" : passport_make_date.strftime("%Y-%m-%d %H:%M:%S"),"passport_expiry_date" : passport_expiry_date.strftime("%Y-%m-%d %H:%M:%S"),"ipaddress" : ipaddress , "mobile" : mobile}

                print "Sending: " , payload , "\n"

                if service == 'kinesis':

                    put_response = client.put_record(
                        StreamName=streamName,
                        Data=json.dumps(payload),
                        PartitionKey=ipaddress
                        )
                    time.sleep(2)

                    '''
                    # To Send Multiple Records in one Request - Create a list of N payloads list.append(payload)
                    payload_list = []
                    for i in range(5):
                        payload_list.append(payload)
                    put_response = client.put_records(
                        StreamName=streamName,
                        Records=[{
                            'Data' : json.dumps(payload_list),
                            'PartitionKey' : ipaddress
                        },]
                        )
                    '''

                else:

                    put_response = client.put_record(
                        DeliveryStreamName=streamName,
                        Record = {
                        'Data' : json.dumps(payload)
                        })
                    time.sleep(2)

                    '''
                    # To Send Multiple Records in one Request - Create a list of N payloads list.append(payload)
                    payload_list = []
                    for i in range(5):
                        payload_list.append(payload)

                    put_response = client.put_record_batch(
                        DeliveryStreamName=streamName,
                        Records=[{
                            'Data': json.dumps(payload_list)
                        },]
                        )
                    '''

                if put_response['ResponseMetadata']['HTTPStatusCode'] == 200:
                        print "Payload Sent Successfully !!\n"
                else:
                        print "\nSomething WRONG with the Response received. HTTP Status Code: %s !!" %(put_response['ResponseMetadata']['HTTPStatusCode'])

        except Exception,e:
                print "\nFailed to Send Records to Given Stream !!"
                print "\n       Exiting with ERROR:"
                print "\n       " , e , "\n"
                sys.exit()


# Main Function
if __name__ == '__main__':

        # Check Number of Arguments
        if len(sys.argv) == 2 and sys.argv[1].lower() in ['h', 'help', 'usage']:
                usage()
        elif len(sys.argv) != 3:
                print "\n       Invalid Number of Arguments. Expecting Service and Stream Name !!"
                usage()

        service = sys.argv[1]
        streamName = sys.argv[2]

        if service not in ['firehose','kinesis']:
                print "\n       Invalid Service Name in First Argument. Choose from 'firehose' or 'kinesis' !!"
                usage()


        client = boto3.client(
                service,
                region_name='us-east-1',
                aws_access_key_id='yourAccessKeyID',
                aws_secret_access_key='yourSecretAccesskey',
                )

        print "\nChecking Stream Existence"
        check_stream_existence(service, client, streamName)

        print "\nGenerate the Payload for Given Stream"
        produce_messages(service, client, streamName)
