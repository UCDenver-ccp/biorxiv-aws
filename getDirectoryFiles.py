import json
import boto3


def lambda_handler(event, context):
    if 'body' in event:
        body = json.loads(event['body'])
    else:
        body = event
    bucket = body['source-bucket']
    directory_prefix = body['directory-prefix']
    client = boto3.client('s3')
    file_name_list = []
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
            full_path = contents['Key']
            file_part = full_path.replace(directory_prefix, '')
            if not full_path.endswith('.meca') or '/' in file_part:
                continue
            file_name_list.append(full_path)
    return {
        'statusCode': 200,
        'body': {
            "file_count": len(file_name_list),
            "paths": file_name_list
        }
    }
