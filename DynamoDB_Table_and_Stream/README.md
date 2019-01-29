
# DynamoDB Table, DynamoDB Stream and DynamoDB Stream Processor using Lambda

## Purpose
#### DynamoDB_Single_Rec_Insert_Modify_Delete.py:
    To Create required DynamoDB Table (if Doesn't exists) with 
        - email and Ipaddress as Primary and Sort key respectively
        - NEW_IMAGE as StreamViewType
    and finally Insert/Modify/Delete a Single Record.
#### Accessing_DynamoDB_Stream.py:
    To access Dynamo DB Stream for the given Table
    Print all information based on the StreamViewType of the Stream.
#### DynamoDB_Create_Table_and_Insert_Records.py:
    To Create required DynamoDB Table (if Doesn't exists) with 
        - email and Ipaddress as Primary and Sort key respectively
        - NEW_IMAGE as StreamViewType
     Insert JSON messages every 2 second
#### DynamoDBStream_Processor_Lambda.py: Replicating a DynamoDB Table in another Region
    To Consume messages from a DynamoDB Stream.
    Filter out messages having Valid Email Address
    Check and Create(if required) DynamoDB Table 'users' is present in another Region - ap-southeast-2
    Load the 'users' Table with valid records (having Valid Email Address).

## Prerequisites
Packages: 
```
curl -O https://bootstrap.pypa.io/get-pip.py
python get-pip.py
pip install awscli==1.15.83
pip install boto3

Script should be updated with Required AWS Access/Secret Key.
```

## Usage
```
[root@apache-hadoop ~]# python DynamoDB_Single_Rec_Insert_Modify_Delete.py help

       python DynamoDB_Single_Rec_Insert_Modify_Delete.py TableName

[root@apache-hadoop ~]# python Accessing_DynamoDB_Stream.py help

       python Accessing_DynamoDB_Stream.py TableName

[root@apache-hadoop ~]# python DynamoDB_Create_Table_and_Insert_Records.py help

       python DynamoDB_Create_Table_and_Insert_Records.py TableName
```

## Contact
nrsh13@gmail.com
