import boto3
import json


def lambda_handler(event, context):
    print(event)
    if 'body' in event:
        body = json.loads(event['body'])
    else:
        body = event
    bucket = body['bucket']
    directory_prefix = body['subdirectory'] if 'subdirectory' in body else ''
    print('prefix: ' + directory_prefix + '\n\n')
    client = boto3.client('s3')
    directory_list = []
    next_token = 'init'
    while next_token:
        if next_token != 'init':
            response = client.list_objects_v2(Bucket=bucket, Prefix=directory_prefix, ContinuationToken=next_token, RequestPayer='requester')
        else:
            response = client.list_objects_v2(Bucket=bucket, Prefix=directory_prefix, RequestPayer='requester')
        next_token = response['NextContinuationToken'] if 'NextContinuationToken' in response else None
        if 'Contents' not in response:
            break
        for contents in response['Contents']:
            if contents['Key'].endswith('/'):
                directory_list.append(contents['Key'])
    return {
        'statusCode': 200,
        'body': {
            'directories': directory_list
        }
    }
