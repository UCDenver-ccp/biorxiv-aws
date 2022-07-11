import json
import boto3
import os
import shutil
import time
from zipfile import ZipFile
from zipfile import BadZipFile

MAX_TIME = 14.75 * 60  # The max timeout for Lambda functions is 15 minutes so we'll try to be done in 14m 45s


def clear_and_build_directories():
    if os.path.isdir('/tmp/meca'):
        shutil.rmtree('/tmp/meca')
    os.mkdir('/tmp/meca')
    if os.path.isdir('/tmp/xml'):
        shutil.rmtree('/tmp/xml')
    os.mkdir('/tmp/xml')


def download_archive(client, bucket, remote_filepath):
    file_part = remote_filepath.split('/')[-1]
    if not os.path.isfile('/tmp/meca/' + file_part):
        with open('/tmp/meca/' + file_part, 'wb') as dest:
            client.download_fileobj(bucket, remote_filepath, dest, ExtraArgs={'RequestPayer': 'requester'})
    return '/tmp/meca/' + file_part


def extract_xml_file(archive_filename, output_directory, prefix='content/'):
    try:
        with ZipFile(archive_filename) as archive_file:
            for name in archive_file.namelist():
                if name.startswith(prefix) and name.endswith('.xml'):
                    return archive_file.extract(name, output_directory)
    except BadZipFile as bzp:
        print(f"Could not extract from {archive_filename}")
        print(bzp)
    return None


def lambda_handler(event, context):
    start = time.time()
    if 'body' in event:
        body = json.loads(event['body'])
    else:
        body = event
    client = boto3.client('s3')
    clear_and_build_directories()

    source_bucket = body['source-bucket']
    destination_bucket = body['destination']
    destination_prefix = body['directory']
    gcp_key_id = body['key_id']
    gcp_secret = body['secret']

    gcp_client = boto3.client(
        's3',
        region_name='auto',
        endpoint_url='https://storage.googleapis.com',
        aws_access_key_id=gcp_key_id,
        aws_secret_access_key=gcp_secret
    )

    success_dict = {}
    error_list = []

    for filepath in body['paths']:
        if not filepath.endswith('.meca'):
            error_list.append(filepath)
            continue
        local_filepath = download_archive(client, source_bucket, filepath)
        xml_filename = extract_xml_file(local_filepath, '/tmp/xml/')
        file_part = xml_filename.split('/')[-1]
        gcp_client.upload_file(xml_filename, destination_bucket, destination_prefix + file_part)
        os.remove(local_filepath)
        os.remove(xml_filename)
        success_dict[filepath] = file_part
        if time.time() - start >= MAX_TIME:
            break

    return {
        'statusCode': 200,
        'body': {
            'downloaded_files': success_dict,
            'error_files': error_list,
            'runtime': time.time() - start
        }
    }
