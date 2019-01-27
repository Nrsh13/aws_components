import boto3, sys, json
import sys, signal

def usage():
        print "\nStream Name is Missing. Pass as an Argument. Exiting !!"
        print "\n       python "+sys.argv[0]+" kStreamName\n"
        sys.exit()


def signal_handler(signal, frame):
    print("\n\nCTRL+c  Pressed. Program Exiting Gracefully !!\n")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


def collect_prereq(client,kStream):
        try:
                print "\n# Listing the Shards for given STREAM ==>"
                response = client.list_shards(StreamName=kStream)
                shardid = response['Shards'][0]['ShardId']

                print "\n# Getting Shard Iterator !!"

                response = client.get_shard_iterator(
                        StreamName=kStream,
                        ShardId=shardid,
                        ShardIteratorType='LATEST',
                        )
                sharditerator = response['ShardIterator']
                return sharditerator

        except Exception,e:
                print "\nFailed to Collect Pre-requisite - shardIterator !!"
                print "\nExiting with ERROR:"
                print "\n " , e , "\n"
                sys.exit()



def fetch_stream_records(client, shardIterator):
        print "\n# Getting Records From the Stream ==>"
        response = client.get_records( ShardIterator=shardIterator, Limit=10 )

        while 'NextShardIterator' in response:
          try:
                response = client.get_records(ShardIterator=response['NextShardIterator'], Limit=10)
                for i in range(len(response['Records'])):
                        print "\n", response['Records'][i]['Data']
          except Exception,e:
                print "\nFailed to Fetch Stream Records !!"
                print "\nExiting with ERROR:"
                print "\n " , e , "\n"
                sys.exit()


if __name__ == "__main__":
        if  len(sys.argv) != 2:
                usage()
        kStream =  sys.argv[1]

        client = boto3.client(
                        'kinesis',
                        region_name='us-east-1',
                        aws_access_key_id='yourAccessKeyID',
                        aws_secret_access_key='yourSecretAccessKey',
                        )

        sharditerator = collect_prereq(client, kStream)

        fetch_stream_records(client, sharditerator)