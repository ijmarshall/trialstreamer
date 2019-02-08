#
#   ICTRP download updater
#


from trialstreamer import dbutil, config
import trialstreamer
import boto3
import logging
import os
import re
import datetime
import tqdm
import xml.etree.cElementTree as ET
import subprocess
import sys
import json


log = logging.getLogger(__name__)


def get_date_from_ictrp_fn(fn):
    m = re.match('ictrp\-raw\-([0-9]{4})\-w([0-9]{1,2})', fn)
    if m:
        return datetime.datetime.strptime(
        '{:04d} {:02d} {:d}'.format(int(m.group(1)), int(m.group(2)), 1),
        '%G %V %u')
    else:
        return None


log.info('Connecting to S3')
# get s3 connection
s3 = boto3.resource('s3', region_name='eu-central-1', aws_access_key_id=config.AWS_KEY, aws_secret_access_key=config.AWS_SECRET)

# find out the name of the most recent data file
bucket = s3.Bucket('ictrp-data')

def check_if_new_data():

    # first find out our most recent filename and date
    lu = dbutil.last_update("ictrp")

    fns = []

    for obj in bucket.objects.iterator():
        ext = os.path.splitext(obj.key)[1]
        if ext==".gz":
            dt = get_date_from_ictrp_fn(obj.key)
            if dt:
                fns.append({"fn": obj.key, "date": dt})

    most_recent_fn = sorted(fns, key=lambda x: (x['date']), reverse=True)[0]

    if lu is None or (lu['source_date'] < most_recent_fn['date']):
        log.info('New update available')
        return most_recent_fn

    else:
        log.info('No new updates available')
        return None



def download_s3(fn):
    local_target = os.path.join(trialstreamer.DATA_ROOT, 'ictrp', fn)
    bucket.download_file(fn, local_target)
    return local_target

def parse_file(fn):
    cmd = "{} {}".format(os.path.join(config.ICTRP_RETRIEVAL_PATH, 'parse.py'), fn)
    with open('test.log', 'wb') as f:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, cwd=config.ICTRP_RETRIEVAL_PATH)
        for line in tqdm.tqdm(iter(process.stdout.readline, b'')):  # replace '' with b'' for Python 3
            yield json.loads(line.decode('utf-8'))

def upload_to_postgres(fn):
    cur = dbutil. db.cursor()
    cur.execute("DELETE FROM ictrp;")

    for entry in tqdm.tqdm(parse_file(fn), desc="parsing ICTRP entries"):
        row = (entry['study_id'], entry['scientific_title'],
                json.dumps(entry), fn)

        cur.execute("INSERT INTO ictrp (regid, ti, ictrp_data, source_filename) VALUES (%s, %s, %s, %s);",
            row)
    cur.close()
    dbutil.db.commit()

def add_year():
    """
    add the year information for record creation
    """
    cur = dbutil. db.cursor()
    cur.execute("update ictrp set year=left(ictrp_data->>'date_registered', 4);")
    cur.close()
    dbutil.db.commit()



def update():
    log.info('checking for any new data')
    fn = check_if_new_data()
    if fn is None:
        log.info('no new data found')
        return None
    log.info('found new file {}... downloading'.format(fn['fn']))
    local_fn = download_s3(fn['fn'])
    download_date = datetime.datetime.now()
    log.info('downloaded to {}'.format(local_fn))
    log.info('Parsing to postgres {}'.format(local_fn))
    upload_to_postgres(local_fn)
    dbutil.log_update(update_type='ictrp', source_filename=local_fn, source_date=fn['date'], download_date=download_date)
    log.info('adding year of record creation information')
    add_year()

def upload_old_file(local_fn, force_update=False):
    if force_update==False:
        log.warning('This will delete the existing ICTRP database. Please run with force_update=True to continue.')
        return None
    local_fn = os.path.join(trialstreamer.DATA_ROOT, 'ictrp', local_fn)
    download_date = datetime.datetime.now()   
    log.info('Parsing to postgres {}'.format(local_fn))
    upload_to_postgres(local_fn)
    # dbutil.log_update(update_type='ictrp', source_filename=local_fn, source_date=get_date_from_ictrp_fn(local_fn), download_date=download_date)
    log.info('adding year of record creation information')
    add_year()
















def main():
    pass



if __name__ == '__main__':
    main()
