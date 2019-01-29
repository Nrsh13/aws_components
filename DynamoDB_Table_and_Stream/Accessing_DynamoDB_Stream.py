import boto3, sys, json, signal

def usage():
        print "\n       python "+sys.argv[0]+" TableName\n"
        sys.exit()

		
def signal_handler(signal, frame):
    print("\n\nCTRL+c  Pressed. Program Exiting Gracefully !!\n")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


def collect_prereq(client,table):

        try:
                print "\n# Listing Latest STREAMS for Table table ==>"
                response = client.list_streams( TableName=table, Limit=1 )

                print "\n# Fetch the StreamArn for DESCRIBE STREAM !! "
                streamArn = response['Streams'][0]['StreamArn']


                print "\n# Describing STREAMS for Table 'users' ==>"
                response = client.describe_stream( StreamArn=streamArn, Limit=1 )
                print "\n" , response['StreamDescription']


                print "\n# Fetch the ShardId for SHARD Iterator and StreamViewType for GETTING Record !!"
                shardId = response['StreamDescription']['Shards'][0]['ShardId']
                streamViewType = response['StreamDescription']['StreamViewType']


                print "\n# Getting Shard Iterator !!"
                response = client.get_shard_iterator( ShardId=shardId, ShardIteratorType='TRIM_HORIZON', StreamArn=streamArn )
                shardIterator =  response['ShardIterator']

                return streamViewType, shardIterator

        except Exception,e:
                print "\nFailed to Collect Pre-requisites like streamArn, shardId, shardIterator !!"
                print "\nExiting with ERROR:"
                print "\n " , e
                sys.exit()


def print_other_info(streamViewType,response,i):
        try:
                print "\n Stream View Type: " , streamViewType
                print " AWS Region: " , response['Records'][i]['awsRegion']
                print " Event Name: " , response['Records'][i]['eventName']
                print " Even Source: " , response['Records'][i]['eventSource']
        except Exception,e:
                print "\nFailed to Print Other Info like Region, Event Name(Insert/Modify/Remove) !!"
                print "\nExiting with ERROR:"
                print "\n " , e
                sys.exit()


def fetch_stream_records(client, streamViewType, shardIterator):

        print "\n# Getting Records From the Stream ==>"
        response = client.get_records( ShardIterator=shardIterator, Limit=100 )
        #print response
        try:
            if streamViewType == 'KEYS_ONLY':
                for i in range(len(response['Records'])):
                        print_other_info(streamViewType,response,i)
                        print " KEYS are: \n"
                        print (json.dumps(response['Records'][i]['dynamodb']['Keys'], indent=4, sort_keys=True)) , "\n"

            elif streamViewType == 'NEW_IMAGE':
                for i in range(len(response['Records'])):
                        print_other_info(streamViewType,response,i)
                        print " KEYS are: \n"
                        if response['Records'][i]['eventName'] != 'REMOVE':
                                print (json.dumps(response['Records'][i]['dynamodb']['NewImage'], indent=4, sort_keys=True)) , "\n"
                        else:
                                print (json.dumps(response['Records'][i]['dynamodb']['Keys'], indent=4, sort_keys=True)) , "\n"
                                print "Above KEY has been Removed !!\n"

            elif streamViewType == 'OLD_IMAGE':
                for i in range(len(response['Records'])):
                        print_other_info(streamViewType,response,i)
                        print " KEYS are: \n"
                        try:
                                print (json.dumps(response['Records'][i]['dynamodb']['OldImage'], indent=4, sort_keys=True)) , "\n"
                        except:
                                print "A Total NEW RECROD found which will NOT have any OLD Image !!\n"

            elif streamViewType == 'NEW_AND_OLD_IMAGES':
                for i in range(len(response['Records'])):
                        print_other_info(streamViewType,response,i)
                        print " KEYS are: \n"
                        try:
                                if i != (len(response['Records']) -1):
                                    print (json.dumps(response['Records'][i]['dynamodb']['NewImage'], indent=4, sort_keys=True)) , "\n"
                                    print (json.dumps(response['Records'][i]['dynamodb']['OldImage'], indent=4, sort_keys=True)) , "\n"
                                else:
                                    print (json.dumps(response['Records'][i]['dynamodb']['OldImage'], indent=4, sort_keys=True)) , "\n"
                                    print (json.dumps(response['Records'][i]['dynamodb']['NewImage'], indent=4, sort_keys=True)) , "\n"
                        except:
                                if i == 0:
                                    print "For FIRST RECORD, OLD Image Will NOT Be there !!"
                                else:
                                    print "For LAST RECORD, NEW Image Will NOT Be there !!\n"

        except Exception,e:
                print "\nFailed to Fetch Stream Records !!"
                print "\nExiting with ERROR:"
                print "\n " , e
                sys.exit()


if __name__ == "__main__":

        # Check Number of Arguments
        if  len(sys.argv) != 2:
                print "\nTable Name Missing. Pass as an Argument. Exiting !!"
                usage()
        elif len(sys.argv) == 2 and sys.argv[1].lower() in ['h', 'help', 'usage']:
                usage()

        table =  sys.argv[1]

        client = boto3.client('dynamodbstreams',
                    aws_access_key_id='yourAccessKeyID',
                    aws_secret_access_key='yourSecretAccessKey',
                    region_name='us-east-1')

        streamViewType, shardIterator = collect_prereq(client, table)

        fetch_stream_records(client, streamViewType, shardIterator)