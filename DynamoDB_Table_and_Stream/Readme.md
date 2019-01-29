
# Kinesis Stream, Firehose Delivery Stream and Kinesis Stream Processor using Lambda

## Purpose
#### kinesis_json_producer_boto3.py:
    Kinesis Stream: To Create a Kinesis Stream (if Doesn't exist) and Produce random JSON messages.
    Firehose: To Produce random JSON messages to an Existing Firehose Deliver Stream.
#### kinesis_json_consumer_boto3.py:
    To Consume messages from a Kinesis Stream/Firehose Delivery Stream
#### kinesisStream_Processor_Lambda.py:
    To Consume messages from a Kinesis Stream.
    Filter out messages having Valid Email Address
    Check and Create(if required) DynamoDB Table 'users'
    Load the 'users' Table with valid records (having Valid Email Address).

## Prerequisites
Packages: 
```
curl -O https://bootstrap.pypa.io/get-pip.py
python get-pip.py
pip install awscli==1.15.83
pip install boto3

Firehose Delivery Stream should exists.
Script should be updated with Required AWS Access/Secret Key.
```

## Usage
### Producer
```
[root@apache-hadoop ~]# python kinesis_json_producer_boto3.py help

    # Firehose: The Delivery Stream should exist. This Script will Produce Messages to the given Deilvery Stream.
    # Kinesis Stream: This Script will CREATE a STREAM (if Doesn't Exist) and Produce Messages:

             Usage: python kinesis_json_producer_boto3.py 'firehose|kinesis' 'DeliveryStreamName|kStreamName'
```
### Consumer
```
[root@apache-hadoop ~]# python kinesis_json_consumer_boto3.py help

       python kinesis_json_consumer_boto3.py kStreamName

```

## Example
Producer Console
```
[root@apache-hadoop ~]# python kinesis_json_producer_boto3.py kinesis mystream

# Checking Stream Existence

# Kinesis Stream 'mystream' Exists !!

# Generate the Payload for Given Stream

# Producing N Messages sys.maxint. Press CTRL+C to STOP

Sending:  {'lname': 'Scott', 'passport_expiry_date': '2025-07-28 14:12:05', 'passport_make_date': '2015-07-28 14:12:05', 'fname': 'Louis', 'mobile': '99858575555', 'ipaddress': '17.181.43.65', 'email': 'Louis_Scott@rediffmail.com', 'principal': 'Louis@EXAMPLE.COM'}

Payload Sent Successfully !!

Sending:  {'lname': 'Long', 'passport_expiry_date': '2023-02-27 14:12:07', 'passport_make_date': '2013-02-27 14:12:07', 'fname': 'Michael', 'mobile': 9876062095, 'ipaddress': '220.111.77.112', 'email': 'Michael_Long@hcl.com', 'principal': 'Michael@EXAMPLE.COM'}

Payload Sent Successfully !!

Sending:  {'lname': 'Sanchez', 'passport_expiry_date': '2022-06-29 14:12:10', 'passport_make_date': '2012-06-29 14:12:10', 'fname': 'Anthony', 'mobile': 9802022300, 'ipaddress': '7.182.167.181', 'email': 'Anthony_Sanchez@rediffmail.com', 'principal': 'Anthony@EXAMPLE.COM'}

^C

CTRL+c  Pressed. Program Exiting Gracefully !!
```

Consumer Console
```
[root@apache-hadoop ~]# python kinesis_json_consumer_boto3.py mystream

# Listing the Shards for given STREAM ==>

# Getting Shard Iterator !!

# Getting Records From the Stream ==>

{"lname": "Scott", "passport_expiry_date": "2025-07-28 14:12:05", "passport_make_date": "2015-07-28 14:12:05", "fname": "Louis", "mobile": "99858575555", "ipaddress": "17.181.43.65", "email": "Louis_Scott@rediffmail.com", "principal": "Louis@EXAMPLE.COM"}

{"lname": "Long", "passport_expiry_date": "2023-02-27 14:12:07", "passport_make_date": "2013-02-27 14:12:07", "fname": "Michael", "mobile": 9876062095, "ipaddress": "220.111.77.112", "email": "Michael_Long@hcl.com", "principal": "Michael@EXAMPLE.COM"}

{"lname": "Sanchez", "passport_expiry_date": "2022-06-29 14:12:10", "passport_make_date": "2012-06-29 14:12:10", "fname": "Anthony", "mobile": 9802022300, "ipaddress": "7.182.167.181", "email": "Anthony_Sanchez@rediffmail.com", "principal": "Anthony@EXAMPLE.COM"}
^C

CTRL+c  Pressed. Program Exiting Gracefully !!
```
## Contact
nrsh13@gmail.com
