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
from robotreviewer.textprocessing import minimap
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



non_rcts = ['allocation : not applicable',
 'assignment: other',
 'before after control',
 'case control',
 'case control',
 'case control study',
 'case series',
 'case study',
 'cluster randomly sampling',
 'cohort study',
 'control: historical',
 'cross sectional',
 'cross sectional',
 'cross sectional study',
 'diagnostic accuracy study',
 'diagnostic test for accuracy',
 'duration: longitudinal',
 'epidemiological study',
 'historical control',
 'logitudinal',
 'longitudinal study  treatment ',
 'mixed methods',
 'n of 1 trial',
 'non comparative',
 'non randomised trial',
 'non randomized control',
 'non randomized controlled trial',
 'non rct',
 'not randomized',
 'observational',
 'observational study',
 'open label',
 'open label',
 'pre post',
 'purpose: natural history',
 'qualitative',
 'quasi experimental',
 'quasi randomized controlled',
 'randomised: no',
 'randomization sequence:not applicable',
 'randomization sequence:other',
 'randomization: n a',
 'randomly sampling',
 'retrospective',
 'sequential',
 'single arm',
 'single arm',
 'single group assignment',
 'survey',
 'uncontrolled']

rcts = ['adaptive randomization',
 'allocation : rct',
 'assignment: crossover',
 'cluster controlled trial',
 'cluster randomization',
 'computer generated randomization',
 'cross over',
 'crossover trial',
 'double blind',
 'double masked',
 'experimental',
 'factorial',
 'interventional trial',
 'parallel',
 'permuted block randomization',
 'phase 1',
 'phase 2',
 'phase 3',
 'phase 4',
 'phase i',
 'phase ii',
 'phase iii',
 'phase iv',
 'pilot rct',
 'ramdomised controlled trial',
 'rct',        
 'random allocation',
 'random number table',
 'randomised',
 'randomise',
 'randomised controlled trial',
 'randomization sequence:coin toss  lottery  toss of dice  shuffling cards',
 'randomize',
 'randomized',
 'randomized controlled trial',
 'single centre trial',
 'stratified block randomization',
 'stratified randomization']

def cleanup(raw):
    txt = re.sub("[^a-zA-Z\d]", " ", raw)
    txt = re.sub("\s\s+", " ", txt)
    return txt

def is_recruiting(recruitment_status):
    if recruitment_status == "Recruiting":
        return "recruiting"
    elif recruitment_status=="Not Recruiting":
        return "not recruiting"
    else:
        return "unknown"

def is_rct(study_design):
    """
    rules of thumb for finding RCTs
    based on analysis of unique study_design fields conducted on 2020-03-29
    """
    if study_design is None:
        return "unknown"

    sd_clean = cleanup(study_design.lower())

    if any((r in sd_clean for r in non_rcts)):
        # first get the definite no's    
        return "non-RCT"
    elif any((r in sd_clean for r in rcts)):
        # then get the likely yes's    
        return "RCT"
    else:
        return "unknown"



def parse_ictrp(ictrp_data):
    

        
    out = {"regid": ictrp_data['study_id']}

    try:
        out["ti"] = ictrp_data['scientific_title'].strip()
    except:
        out["ti"] = "unknown"

    try:        
        out["population"] = [r.get('description', '').strip() for r in ictrp_data.get("health_conditions", [])]
    except:
        out["population"] = []

    try:        
        out["interventions"] = [r.get('description', '').strip() for r in ictrp_data.get("interventions", [])]
    except:
        out["interventions"] = []

    try:        
        out["outcomes"] = [r.get('description', '').strip() for r in ictrp_data.get("outcomes", [])]
    except:
        out["outcomes"] = []

    try:
        out["is_rct"] = is_rct(ictrp_data.get('study_design'))
    except:
        out['is_rct'] = "unknown"

    try:
        out["is_recruiting"] = is_recruiting(ictrp_data.get("recruitment_status"))
    except:
        out["is_recruiting"] = "unknown"

    try:
        out["target_size"] = str(int(ictrp_data['target_size']))
        if len(out["target_size"]) > 10:
            out['target_size'] = "unknown"    
    except:
        out['target_size'] = "unknown"

    try:
        out["date_registered"] = datetime.datetime.strptime(ictrp_data['date_registered'], "%Y-%m-%d")
    except:
        out["date_registered"] = None

    try:
        out['year'] = out['date_registered'].year
    except:
        out['year'] = None

    try:
        out["countries"] = ictrp_data['countries']
    except:
        out["countries"] = []

    for f in ['population', 'interventions', 'outcomes']:
        try:
            out[f"{f}_mesh"] = minimap.get_unique_terms((cleanup(o_i) for o_i in out[f] if o_i))
        except:
            out[f"{f}_mesh"] = []
        
    return out


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
    local_target = os.path.join(config.ICTRP_DATA_PATH, fn)
    bucket.download_file(fn, local_target)
    return local_target

def parse_file(fn):
    cmd = "{} {}".format(os.path.join(trialstreamer._ROOT, config.ICTRP_RETRIEVAL_PATH, 'parse.py'), fn)
    print(cmd)
    with open('test.log', 'wb') as f:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, cwd=os.path.join(trialstreamer._ROOT, config.ICTRP_RETRIEVAL_PATH))
        for line in tqdm.tqdm(iter(process.stdout.readline, b'')):  # replace '' with b'' for Python 3
            yield json.loads(line.decode('utf-8'))

def upload_to_postgres(fn, force_update=False):


    cur = dbutil.db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if force_update:
        cur.execute("DELETE FROM ictrp;")
        already_done = set()
    else:
        cur.execute("SELECT regid from ictrp;")
        already_done = set((r['regid'] for r in cur))


    for i, entry in tqdm.tqdm(enumerate(parse_file(fn)), desc="parsing ICTRP entries"):

        if i % 500 == 0:
            dbutil.db.commit()

        if entry['study_id'] in already_done:
            continue


        try:
            assert is_rct(entry.get('study_design'))=='RCT'
        except:
            continue

        p = parse_ictrp(entry)
        row = (p['regid'], p['ti'], json.dumps(p['population']), json.dumps(p['interventions']),
            json.dumps(p['outcomes']), json.dumps(p['population_mesh']), 
            json.dumps(p['interventions_mesh']), json.dumps(p['outcomes_mesh']),
            p['is_rct'], p['is_recruiting'], p['target_size'], p['date_registered'], p['year'],
            json.dumps(p['countries']), json.dumps(entry), fn)

        cur.execute("INSERT INTO ictrp (regid, ti, population, interventions, outcomes, population_mesh, interventions_mesh, outcomes_mesh, is_rct, is_recruiting, target_size, date_registered, year, countries, ictrp_data, source_filename) VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
            row)

        already_done.add(entry['study_id'])


    cur.close()
    dbutil.db.commit()

def add_year():
    """
    add the year information for record creation
    """
    cur = dbutil. db.cursor()
    cur.execute("update ictrp set year=left(ictrp_data->>'date_registered', 4)::int;")
    cur.close()
    dbutil.db.commit()


def update(force_update=False):
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
    upload_to_postgres(local_fn, force_update=force_update)
    dbutil.log_update(update_type='ictrp', source_filename=local_fn, source_date=fn['date'], download_date=download_date)
    log.info('adding year of record creation information')
    add_year()

def upload_old_file(local_fn, force_update=False):
    local_fn = os.path.join(config.ICTRP_DATA_PATH, local_fn)
    download_date = datetime.datetime.now()
    log.info('Parsing to postgres {}'.format(local_fn))
    upload_to_postgres(local_fn, force_update=force_update)
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
