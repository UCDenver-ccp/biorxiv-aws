# Scripts for bioRxiv and medRxiv
A set of scripts for accessing the public AWS S3 buckets for bioRxiv and medRxiv and retrieving full text articles
The scripts `listDirectories.py`, `getDirectoryFiles.py`, and `processFiles.py` are made to be deployed as AWS Lambda functions.
The function URLs for the currently deployed functions are hard-coded in the `runner.py` script.
```
usage: runner.py [-h] [-i INSTANCE] [-d DATABASE] [-u USER] [-p PASSWORD] [-k KEY] [-s SECRET] [-a AWS_KEY] [-w AWS_SECRET] [-t TASK] [-o OLD] [-c CHUNK]

optional arguments:
  -h, --help            show this help message and exit
  -i INSTANCE, --instance INSTANCE
                        GCP DB instance name
  -d DATABASE, --database DATABASE
                        database name
  -u USER, --user USER  database username
  -p PASSWORD, --password PASSWORD
                        database password
  -k KEY, --key KEY     HMAC key id
  -s SECRET, --secret SECRET
                        HMAC secret
  -a AWS_KEY, --aws-key AWS_KEY
                        AWS access key
  -w AWS_SECRET, --aws-secret AWS_SECRET
                        AWS secret key
  -t TASK, --task TASK  task to execute
  -o OLD, --old OLD     previous scan date to rescan (ISO 8601 date)
  -c CHUNK, --chunk CHUNK
                        size of file processing chunks
```
The runner script will look for a file named ```prod-creds.json``` in the working directory, which should be a valid credentials file with permissions to access the Google Cloud Database.
As currently set up, the AWS Lambda functions require AWS_IAM authentication provided by the AWS_KEY and AWS_SECRET parameters.
The destination GCS bucket is currently hard-coded and not publically accessible, so the HMAC KEY and SECRET are needed to allow AWS to transfer files.

