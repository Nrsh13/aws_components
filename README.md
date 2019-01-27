# Kinesis Stream & Firehose Delivery Stream JSON Producer and Consumer

## Purpose
Kinesis Stream: To Create a Kinesis Stream (if Doesn't exist) and Produce random JSON messages.

Firehose: To Produce random JSON messages to an Existing Firehose Deliver Stream.

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

## Contact
nrsh13@gmail.com
