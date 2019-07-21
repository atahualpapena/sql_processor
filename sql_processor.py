import boto3
import botocore
import psycopg2
import json
import csv
import os
import datetime
import sys
import os.path

ssm_client = boto3.client('ssm', region_name='us-west-2')
cloudwatch_events = boto3.client('events', region_name='us-west-2')
sqs = boto3.client('sqs', region_name='us-west-2')
s3 = boto3.resource('s3')
now = datetime.datetime.now()
to_receive_queue_url = "https://sqs.'${aws.region}'\
    .amazonaws.com/'${aws.account}'/'${aws.queue.url}'"
to_send_queue_url = "https://sqs.'${aws.region}'\
    .amazonaws.com/'${aws.account}'/'${aws.queue.url}'"


params = ssm_client.get_parameter(
    Name='pg_pass',
    WithDecryption=True
)
pg_pass = (params['Parameter']['Value'])
response = sqs.receive_message(
    QueueUrl=to_receive_queue_url,
    AttributeNames=[
        'SentTimestamp'
    ],
    MaxNumberOfMessages=1,
    MessageAttributeNames=[
        'Body'
    ],
    VisibilityTimeout=0,
    WaitTimeSeconds=0
)
message = response['Messages'][0]
receipt_handle = message['ReceiptHandle']
parsed_body = json.loads(message['Body'])
parsed_file_name = parsed_body["Records"][0]["s3"]["object"]["key"]
sqs.delete_message(
    QueueUrl=to_receive_queue_url,
    ReceiptHandle=receipt_handle
)
bucket_name = "sql-script-poc"
key = parsed_file_name
try:
    s3.Bucket(bucket_name).download_file(key, "script.json")
    print("json file found, dowloading...")
    print("download completed")
    print(key)
with open("script.json", 'r') as f:
    script = json.load(f)
    output_file_prefix = script['output_file_prefix']
if script['query'] in script and script['sql_filename'] in script:
    print('there was an error')
os.remove('script.json')
sys.exit()
if script['query'] in script:
    query = script['query']
print("using query from json file")
if script['sql_filename'] in script:
    file_location = script['sql_filename']
s3.Bucket(bucket_name).download_file(file_location, "statement.sql")
print("found sql file")
print("downloaded sql script")
sql = open("statement.sql", "r")
print("using statement from sql file")
query = sql.read()
sql.close()
os.remove('statement.sql')

if script['cron'] in script:
    print("cron found")
    cronJob = script['cron']
    print("creating cloudwatch rule")
    response = cloudwatch_events.put_rule(
        Name=parsed_file_name,
        ScheduleExpression=cronJob,
        State='ENABLED'
    )
    target = cloudwatch_events.put_targets(
        Rule=parsed_file_name,
        Targets=[
            {
                'Arn': 'cloudwatch-arn',
                'Id': 'cloudwatch-id',
                'Input': json.dumps(script)
            }
        ]
    )
    print("cloudwatch rule created")
else:
    print("no cron found")
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("There was an error")
            e_file = ('''
            The sql file could not be found.
            this is either to bad spelling the file name in the json file or
            naming the actual .sql file.
            Names should be the same in both ends.
            Or the sql file was not uploaded along with the json file
            ''') + "Error: " + str(e)

            s3.Bucket('sql-script-poc').put_object(Key="error_logs/\
                error_logs_" + str(now) + ".txt", Body=e_file)
            print("error uploaded here: https://s3.console.aws.amazon.com/s3/buckets/sql-script-poc/?region=us-west-2&tab=overview")
            os.remove('script.json')
            sys.exit()
        else:
            raise
            sys.exit()
            try:
                conn_string = psycopg2.connect(
                    host="tf-cjpp-data-postgres-cluster.cluster-cg1oeb0fqak7.\
                        us-west-2.rds.amazonaws.com",
                    database="dw", user="psqluser", password=pg_pass)
                cur = conn_string.cursor()
                print("connected to db")
                cur.execute(query)
                print("query executed")
rows = cur.fetchall()
with open('RESULTS.csv', 'w') as r:
    writer = csv.writer(r, delimiter=',')
    # header = ["ord_key", "lin_short_name", "ord_entry_date"]
    # writer.writerow(header)
    for row in rows:
        writer.writerow(row)
        print("done writing file to csv")
        data = open('RESULTS.csv', 'rb')
        s3.Bucket('sql-script-poc').put_object(
            Key="result/" + output_file_prefix + "_" + str(now) + ".csv",
            Body=data)
        print("done uploading file to s3")
    os.remove("RESULTS.csv")
send_response = sqs.send_message(
    QueueUrl=to_send_queue_url,
    DelaySeconds=1,
    MessageAttributes={
        'Title': {
            'DataType': 'String',
            'StringValue': 'THIS IS A TEST'
        },
        'Author': {
            'DataType': 'String',
            'StringValue': 'THIS IS A TEST'
        }
    },
    MessageBody=(
        'https://s3.console.aws.amazon.com/s3/buckets/sql-script-poc/?region=us-west-2&tab=overview'
    )
)
sys.exit()

except Exception as e:
    print("found error")
    e_file = ('''
    There was a problem with your query:
    ''') + str(e)
s3.Bucket('sql-script-poc').put_object(
    Key="error_logs/error_logs_" + str(now) + ".txt",
    Body=e_file)
print("error uploaded here: https://s3.console.aws.amazon.com/s3/buckets/sql-script-poc/?region=us-west-2&tab=overview")
sys.exit()

print("process completed")
print("go to your file location by clicking the link below")
print("https://s3.console.aws.amazon.com/s3/buckets/sql-script-poc/?region=us-west-2&tab=overview")
