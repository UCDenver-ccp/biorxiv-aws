import json
import boto3
import os
import shutil
from zipfile import ZipFile
from zipfile import BadZipFile


# This is currently just a flat file at the root of the bucket, but in the 
# future it may be DB based or one file per directory prefix.
def get_completed_archives(client, bucket, file_name):
    response = client.list_objects_v2(Bucket=bucket, Prefix=file_name, MaxKeys=1)
    if 'Contents' not in response:
        return set()
    with open('/tmp/archive_list.txt', 'wb') as local:
        client.download_fileobj(bucket, file_name, local)
    with open('/tmp/archive_list.txt', 'r') as arc:
        return set(arc.read().splitlines())


def get_completed_archives2(client, bucket, file_name):
    response = client.list_objects_v2(Bucket=bucket, Prefix=file_name, MaxKeys=1)
    if 'Contents' not in response:
        return []
    pairs = []
    with open('/tmp/filename_list.txt', 'wb') as local:
        client.download_fileobj(bucket, file_name, local)
    with open('/tmp/filename_list.txt', 'r') as arc:
        for line in arc.read().splitlines():
            pairs.append((line.split(',')[0], line.split(',')[1]))
    return pairs


def get_bucket_filenames(client, bucket, directory_prefix, file_extension='.meca'):
    next_token = 'init'
    while next_token:
        if next_token != 'init':
            response = client.list_objects_v2(Bucket=bucket, Prefix=directory_prefix, ContinuationToken=next_token, RequestPayer='requester')
        else:
            response = client.list_objects_v2(Bucket=bucket, Prefix=directory_prefix, RequestPayer='requester')
        next_token = response['NextContinuationToken'] if 'NextContinuationToken' in response else None
        if 'Contents' not in response:
            return
        # print(f"{len(response['Contents'])} Contents in response")
        for contents in response['Contents']:
            if file_extension not in contents['Key']:
                continue
            file_size = contents['Size'] // (2**20)
            # print(f"{contents['Key']} : {file_size}")
            # files over 400mb won't be able to be decompressed in the lambda storage, so we have to skip them
            if file_size > 400:
                # print('skipping due to size')
                continue
            yield contents['Key']


def file_exists(client, bucket, filename):
    response = client.list_objects_v2(Bucket=bucket, Prefix=filename)
    print(response)
    return 'Contents' in response


def download_archive(client, bucket, remote_filepath):
    file_part = remote_filepath.split('/')[-1]
    if not os.path.isfile('/tmp/meca/' + file_part):
        # print('Downloading ' + remote_filepath)
        with open('/tmp/meca/' + file_part, 'wb') as dest:
            client.download_fileobj(bucket, remote_filepath, dest, ExtraArgs={'RequestPayer': 'requester'})
    return '/tmp/meca/' + file_part


# Extracts the first XML file in the archive to the output_directory
def extract_xml_files(archive_filename, output_directory, prefix='content/'):
    try: 
        with ZipFile(archive_filename) as archive_file:
            for name in archive_file.namelist():
                if name.startswith(prefix) and name.endswith('.xml'):
                    return archive_file.extract(name, output_directory)
    except BadZipFile as bzp:
        print(f"Could not extract from {archive_filename}")
        print(bzp)
    return None


# This is the entry point in Lambda. The event parameter is the JSON object in Lambda, so it shows up as a Python dict.
def lambda_handler(event, context):
    body = json.loads(event['body'])
    s3_client = boto3.client('s3')
    # total, used, free = shutil.disk_usage("/")
    # print("Total: %d MiB" % (total // (2**20)))
    # print("Used: %d MiB" % (used // (2**20)))
    # print("Free: %d MiB" % (free // (2**20)))

    if os.path.isdir('/tmp/meca'):
        shutil.rmtree('/tmp/meca')
    os.mkdir('/tmp/meca')
    if os.path.isdir('/tmp/xml'):
        shutil.rmtree('/tmp/xml')
    os.mkdir('/tmp/xml')
    source_bucket = body['source-bucket']
    destination_bucket = body['destination-bucket']
    source_prefix = body['directory-prefix']
    destination_prefix = body['output-prefix']
    limit = int(body['limit'])

    namelist = []
    extracted_file_list = []
    uploaded_file_list = []
    archive_set = get_completed_archives(s3_client, destination_bucket, destination_prefix + 'archive-list.txt')
    filename_list = get_completed_archives2(s3_client, destination_bucket, destination_prefix + 'filename-list.txt')

    for remote_file in get_bucket_filenames(s3_client, source_bucket, source_prefix, '.meca'):
        if remote_file.split('/')[-1] in archive_set:
            # print('skipping')
            continue
        # print('attempting to download')
        archive_filename = download_archive(s3_client, source_bucket, remote_file)
        namelist.append(archive_filename)
        # print('Extracting from ' + archive_filename)
        xml_filename = extract_xml_files(archive_filename, '/tmp/xml/')
        if xml_filename is None:
            os.remove(archive_filename)
            continue
        extracted_file_list.append(xml_filename)
        # print(f'Extracted {xml_filename}')
        file_part = xml_filename.split('/')[-1]

        # Check if the XML file already exists in the destination bucket
        existing_files = list(get_bucket_filenames(s3_client, destination_bucket, destination_prefix + file_part, '.xml'))
        if len(existing_files) > 0:
            os.remove(archive_filename)
            os.remove(xml_filename)
            archive_set.add(remote_file.split('/')[-1])
            continue

        # upload and clean up
        archive_set.add(remote_file.split('/')[-1])
        os.remove(archive_filename)
        s3_client.upload_file(xml_filename, destination_bucket, destination_prefix + file_part)
        uploaded_file_list.append(file_part)
        filename_list.append((archive_filename, file_part))
        # print(f'Uploaded {destination_prefix + file_part} to {destination_bucket}')
        os.remove(xml_filename)

        # Exit the loop if we exceeded a non-zero limit
        if 0 < limit <= len(uploaded_file_list):
            break
    # end for loop
    # update the list of processed files
    with open('/tmp/archive-list.txt', 'w') as out:
        for arc in archive_set:
            out.write(arc + '\n')
    s3_client.upload_file('/tmp/archive-list.txt', destination_bucket, destination_prefix + 'archive-list.txt')
    with open('/tmp/filename-list.txt', 'w') as out:
        for pair in filename_list:
            out.write(','.join(pair) + '\n')
    s3_client.upload_file('/tmp/filename-list.txt', destination_bucket, destination_prefix + 'filename-list.txt')
    return {
        'statusCode': 200,
        'body': [x.split('/')[-1] for x in extracted_file_list]
    }
