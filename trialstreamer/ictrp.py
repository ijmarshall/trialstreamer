#
#   ICTRP download updater
#


from trialstreamer import dbutil, config
import trialstreamer
import psycopg2
import psycopg2.extras
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

# in September 2018, this regular expression was developed iteratively, and covered 100%
# of registry IDs from ICTRP
# {'total': 428822, 'matches': 428822}


reg_re = re.compile("""RBR\-[0-9a-z]{6}|\
ACTRN[0-9]{14}|\
ChiCTR\-[A-Za-z]{2,5}\-[0-9]{8}|\
ChiCTR[0-9]{10}|\
IRCT[0-9N]{14,18}|\
PACTR[0-9]{15,16}|\
ISRCTN[0-9]{8}|\
NCT[0-9]{8}|\
CTRI/[0-9]{4}/[0-9]{2,3}/[0-9{6}]|\
DRKS[0-9]{8}|\
EUCTR[0-9]{4}\-[0-9]{6}\-[0-9]{2}|\
JPRN\-C[0-9]{9}|\
JPRN\-JMA\-IIA[0-9]{5}|\
JPRN\-JapicCTI\-{0-9}{6}|\
JPRN\-UMIN[0-9]{9}|\
JPRN\-JapicCTI\-[0-9]{6}|\
KCT[0-9]{7}|\
NTR[0-9]{2,4}|\
PER-[0-9]{3}-[0-9]{2}|\
RPCEC[0-9]{8}|\
SLCTR\/[0-9]{4}/[0-9]{3}|\
TCTR[0-9]{11}""")


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
    loglog.info('Parsing to postgres {}'.format(local_fn))
    upload_to_postgres(local_fn)
    # dbutil.log_update(update_type='ictrp', source_filename=local_fn, source_date=get_date_from_ictrp_fn(local_fn), download_date=download_date)
    log.info('adding year of record creation information')
    add_year()






def compute_registry_links(force_refresh=False, limit_to='is_rct_balanced'):
    """
    compute registry links from all PubMed articles
    """

    if limit_to not in ['is_rct_balanced', 'is_rct_sensitive',
                        'is_rct_precise']:
        raise Exception('limit_to not recognised, needs to be '
                        '"is_rct_precise","is_rct_balanced", or '
                        '"is_rct_sensitive"')

    if limit_to == 'is_rct_sensitive':
        log.warning("Getting metadata for all articles with sensitive cutoff.."
                    "May take a long time...")


    cur = dbutil.db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if force_refresh:
        # deleting doi table from database
        log.info('deleting all registry links from table...')
        cur.execute('delete from registry_links;')
    else:
        log.error('this function always regenerates the index from scratch. please run with force_refresh=True to confirm')
        return None
    log.info('getting pubmed data (from local DB)')
    cur.execute("SELECT pmid, ab, pm_data->'registry_ids' as regids FROM pubmed WHERE {}=true;".format(limit_to))
    records = cur.fetchall()
    for r in tqdm.tqdm(records, desc='processing abstracts'):
        regids = set(r['regids'])
        regids.update(reg_re.findall(r['ab']))
        for regid in regids:
            cur.execute("INSERT INTO registry_links (pmid, regid) VALUES (%s, %s);", (r['pmid'], regid))
    dbutil.db.commit()













def main():
    pass



if __name__ == '__main__':
    main()
