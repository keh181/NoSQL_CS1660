import csv
import boto3

ak = '' #access key
sk = '' #secret key

s3 = boto3.resource('s3', aws_access_key_id=ak,
                    aws_secret_access_key=sk)
try:
    s3.create_bucket(Bucket='datacont-school', CreateBucketConfiguration={'LocationConstraint' : 'us-west-2'})
except:
    print("this already exists")

bucket = s3.Bucket("datacont-school")

bucket.Acl().put(ACL='public-read')

body = open('exp1.csv', 'rb')

o = s3.Object('datacont-school', 'test').put(Body=body)
s3.Object('datacont-school', 'test').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb',
                       region_name='us-west-2',
                       aws_access_key_id=ak,
                       aws_secret_access_key=sk
                       )

try:
    table = dyndb.create_table(
        TableName='DataTable1',
        KeySchema=[
            {
                'AttributeName': 'PartitionKey',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'PartitionKey',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except:
    #if there is an exception, the table may already exist. if so...
    print("Table may exist")
    table = dyndb.Table("DataTable1")

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

print(table.item_count)

with open('experiments.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    next(csvf)
    for item in csvf:
        print(item)
        body = open(item[3], 'rb')
        s3.Object('datacont-school', item[3]).put(Body=body)
        md = s3.Object('datacont-school', item[3]).Acl().put(ACL='public-read')
        url = " https://s3-us-west-2.amazonaws.com/datacont-school/"+item[3]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
                 'description': item[4], 'date': item[2], 'url': url}
        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")


response = table.get_item(
    Key={
        'PartitionKey': 'experiment1',
        'RowKey': 'data1'
    }
)

item = response['Item']
print(item)
