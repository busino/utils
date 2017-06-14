# Raphael Walker, 2017-06-14
#
# Script to be used to backup MySQL db from running docker instance and upload to SOS (Single object storage)
# Daily backups with retention of 20 days
# Monthly backups will be stored forever
#
# Prerequisits
# sudo apt-get install python3-boto3
#
# Add to crontab
# 0 2 * * * /usr/bin/python3 /home/ubuntu/test/backup_db.py > /home/ubuntu/test/log/backup.log 2>&1
#


import sys
import os
import subprocess
import datetime
import argparse
import boto3



KEY = 'EXOwhatever'
SECRET = 'SECRET'
HOST = 'sos.exo.io'
BUCKET_NAME = 'test'
DB_USER = 'test'
DB_PASS = 'test'
DB = 'test'

DAILY_KEY = 'db/daily/{}'
MONTHLY_KEY = 'db/monthly/{}'


def backup():
    '''
    Backup the database and push to Single Object Storage (SOS)
    '''

    print('Backup started: {}'.format(datetime.datetime.now()))

    BACKUP_FILE = '{}_db-{}.sql'.format(DB, datetime.date.today())
    print('Backup database {}'.format(BACKUP_FILE))
    cmd = ['/usr/local/bin/docker-compose exec -T db /usr/bin/mysqldump --user={} --password={} --databases {} > {}'.format(DB_USER, DB_PASS, DB, BACKUP_FILE)]
    return_code = subprocess.call(cmd, shell=True)

    if return_code != 0 or not os.path.exists(BACKUP_FILE):
        print('Error: File {} does not exist or dump failed.'.format(BACKUP_FILE))
        sys.exit(1)

    s3 = boto3.resource('s3',
                        aws_access_key_id=KEY, 
                        aws_secret_access_key=SECRET, 
                        endpoint_url='https://{}'.format(HOST))

    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
    finally:
        bucket = s3.Bucket(BUCKET_NAME)
    
    #
    # backup to daily
    #
    bucket.put_object(Key=DAILY_KEY.format(BACKUP_FILE), Body=open(BACKUP_FILE, 'rb'))
     
    print('File written to daily buckets.')

    #
    # Backup to monthly on the first day of the month
    #
    if datetime.date.today().day == 1:
        bucket.put_object(Key=MONTHLY_KEY.format(BACKUP_FILE), Body=open(BACKUP_FILE, 'rb'))
        print('File written to monthly buckets.')


    #
    # Delete the dump
    #
    os.unlink(BACKUP_FILE)

    #
    # Check daily container and delete old buckets
    #
    now = datetime.datetime.now(datetime.timezone.utc)
    for v in bucket.objects.filter(Prefix='db/daily'):
        if (now - v.last_modified).days > 20:
            print('Bucket {} deleted.'.format(v.name))
            v.delete()


if __name__ == '__main__':

    ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
    print('Change to directory: {}'.format(datetime.datetime.now()))
    os.chdir(ROOT_DIR)

    backup()
