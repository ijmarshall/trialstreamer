#
#   PubMed download updater
#


from trialstreamer import dbutil, config
from robotdata.readers import pmreader
import trialstreamer
import boto3
import logging
import os
import re
import datetime
import hashlib
import tqdm
import xml.etree.cElementTree as ET
import subprocess
import sys
import json
import gzip
import ftplib
import glob
import psycopg2
import collections
from itertools import zip_longest
from robotsearch.robots import rct_robot


with open(os.path.join(trialstreamer.DATA_ROOT, 'rct_model_calibration.json'), 'r') as f:
    clf_cutoffs = json.load(f)



rct_clf = rct_robot.RCTRobot()
log = logging.getLogger(__name__)

homepage = "ftp.ncbi.nlm.nih.gov"

def get_ftp():
    """
    log in to FTP and pass back
    """
    ftp = ftplib.FTP(homepage)
    ftp.login(user="anonymous", passwd=config.PUBMED_USER_EMAIL)
    return ftp

def get_baseline_fns():
    ftp = get_ftp()
    filelist = ftp.nlst('pubmed/baseline/')
    baseline_fns = [f for f in filelist if f.endswith('.gz')]
    return baseline_fns

def already_done_md5s():
    """
    glob which md5s already downloaded
    """
    fns = glob.glob(os.path.join(config.PUBMED_LOCAL_DATA_PATH, '*.md5'))
    return set([os.path.basename(r) for r in fns])

def already_done_gzs():
    """
    glob which gzipped data files already downloaded
    """
    fns =  glob.glob(os.path.join(config.PUBMED_LOCAL_DATA_PATH, '*.gz'))
    return set([os.path.basename(r) for r in fns])

def already_done_clfs():
    """
    glob which classification json files already done
    """
    fns =  glob.glob(os.path.join(config.PUBMED_LOCAL_CLASSIFICATIONS_PATH, '*.json'))
    return set([os.path.basename(r) for r in fns])

def download_ftp_baseline(force_update=False):
    """
    Grab all the latest baseline files (checking if already done first)

    edit safety_test_parse in config file for deployment

    """
    safety_test_parse = config.SAFETY_TEST_PARSE
    if not safety_test_parse:
        log.warning("NB The parse is not being test run... This is quicker, but unexpected code crashes will result in local database being lost")

    baseline_ftp_fns = get_baseline_fns()
    log.info("{} PubMed title/abstract files on FTP server".format(len(baseline_ftp_fns)))
    log.info("Checking hashfiles, and downloading any missing")
    download_md5s(baseline_ftp_fns)
    log.info("Verifying local gzipped data files")
    already_done_fns = already_done_gzs()

    # validate_downloaded_data(already_done_fns)

    log.info("Data validated")
    log.info("Downloading and validating remaining files")
    download_and_validate_gzs(baseline_ftp_fns)
    download_date = datetime.datetime.now()
    log.info("Uploading to postgres")
    upload_to_postgres(baseline_ftp_fns, safety_test_parse, force_update=force_update)
    log.info("Uploaded!")
    dbutil.log_update(update_type='pubmed_baseline', source_filename=os.path.basename(baseline_ftp_fns[0])[:8], source_date=get_date_from_fn(baseline_ftp_fns[0]), download_date=download_date)
    # log.info("Retrieving ptyp info")
    # add_ptyp()

def download_md5s(gz_fns):
    """
    get the hashes from a list of PubMed gziped filenames
    """
    already_done = already_done_md5s()
    ftp = get_ftp()
    for i, gz_fn in enumerate(tqdm.tqdm(gz_fns, desc='md5 hashes downloaded from PubMed FTP server')):
        if os.path.basename(gz_fn) + ".md5" in already_done:
            # skip the already downloaded hashes
            continue
        out_filename = os.path.join(config.PUBMED_LOCAL_DATA_PATH, os.path.basename(gz_fn))
        with open(out_filename + ".md5", 'wb') as f:
            ftp.retrbinary('RETR ' + gz_fn + ".md5", f.write)

def download_and_validate_gzs(gz_fns):
    """
    download datafiles, and validate
    """
    already_done = already_done_gzs()

    for i, gz_fn in enumerate(tqdm.tqdm(gz_fns, desc='data files downloaded from PubMed FTP server')):
        if os.path.basename(gz_fn) in already_done:
            # skip the already downloaded files
            continue
        out_filename = os.path.join(config.PUBMED_LOCAL_DATA_PATH, os.path.basename(gz_fn))

        ftp = get_ftp()

        with open(out_filename, 'wb') as f:
            ftp.retrbinary('RETR ' + gz_fn, f.write)

        with open(out_filename + ".md5", 'wb') as f:
            ftp.retrbinary('RETR ' + gz_fn + ".md5", f.write)

        print(out_filename)

        validate_file(out_filename, out_filename + ".md5", raise_for_errors=True)


def validate_downloaded_data(fns):
    for fn in tqdm.tqdm(fns, desc="Validating local files"):
        fn_path = os.path.join(config.PUBMED_LOCAL_DATA_PATH, fn)
        validate_file(fn_path, fn_path+".md5", raise_for_errors=True)
    return True


def validate_file(fn, hash_fn, raise_for_errors=True):

    with open(fn, 'rb') as f:
        bn = f.read()
    obs_md5 = hashlib.md5(bn).hexdigest()
    with open(hash_fn, 'r') as f:
        true_md5 = (f.read()).rstrip().split("= ")[-1]
    if obs_md5 != true_md5 and raise_for_errors:
        raise Exception("File {} doesn't match md5... possibly corrupted, suggest delete and redownload".format(fn))
    else:
        return (obs_md5 == true_md5)


def iter_abstracts(fn):
    with open(fn, 'rb') as f:
        decompressedFile = gzip.GzipFile(fileobj=f, mode='r')
        for event, elem in ET.iterparse(decompressedFile, events=("start", "end")):
            if elem.tag == "MedlineCitation" and event=="end":
                p_article = pmreader.PubmedCorpusReader(xml_ET=elem)
                yield p_article.to_dict()        

def classify(entry_batch):

    global clf_cutoffs

    thresholds_ptyp = clf_cutoffs['thresholds']['svm_cnn_ptyp']
    thresholds_no_ptyp = clf_cutoffs['thresholds']['svm_cnn']

    threshold_types = ["precise", "balanced", "sensitive"]


  

    X = []

    for entry in entry_batch:
        row = {"title": entry['title'], "abstract": entry['abstract_plaintext'], "ptyp": entry['ptyp']}
        if entry['status'] == 'MEDLINE':
            row['use_ptyp'] = True
        else:
            row['use_ptyp'] = False
        X.append(row)

    preds = rct_clf.predict(X, filter_type="sensitive", filter_class="svm_cnn")

    # prepare data out

    out = []

    for pred in preds:
        row = {"clf_type": pred["model"], "clf_score": pred['score'], "clf_date": datetime.datetime.now(), "ptyp_rct": pred['ptyp_rct']}
        if pred["model"] == "svm_cnn_ptyp":
            for tt in threshold_types:
                row['is_rct_{}'.format(tt)] = (pred['score'] >= thresholds_ptyp[tt])
        elif pred["model"] == "svm_cnn":
            for tt in threshold_types:
                row['is_rct_{}'.format(tt)] = (pred['score'] >= thresholds_no_ptyp[tt])

        out.append(row)
    return out

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks - from itertools recipes adapted a bit"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return ([i for i in r if i is not None] for r in zip_longest(*args, fillvalue=fillvalue))

def get_date_from_fn(ftp_fn):
    """
    The 2018 baseline runs until 31st December 2017
    """
    bn = os.path.basename(ftp_fn)
    return datetime.datetime(int("20" + bn[6:8])-1, 12, 31)

    
def upload_to_postgres(ftp_fns, safety_test_parse, batch_size=500, force_update=False):
    """
    ftp_fns = the filenames to parse (converted to local fns here)
    safety_test_parse = recommended, try and read all the abstracts before deleting the existing data
    batch_size = how many to do at once (often lower = fatster)    
    force_update = whether to delete the database
    """

    num_files = len(ftp_fns)

    if safety_test_parse:
        log.info("Testing parse before inserting into database")

        for idx, ftp_fn in enumerate(ftp_fns):
            local_fn = os.path.join(config.PUBMED_LOCAL_DATA_PATH, os.path.basename(ftp_fn))
            for entry in tqdm.tqdm(iter_abstracts(local_fn), desc="testing the abstract parsing ({}/{}) {}".format(idx, num_files, local_fn)):
                _ = (entry['pmid'], entry['year'], entry['title'], entry['abstract_plaintext'], json.dumps(entry),  
                json.dumps(entry), ftp_fn)

    # if safety mode only delete existing database where the parse has completed without exception
    if force_update:
        log.warning("Deleting all entries from PubMed database")
        cur = dbutil. db.cursor()
        cur.execute("DELETE FROM pubmed;")
        cur.close()
        dbutil.db.commit()

        already_done_pmids = set()
        # dangerous command....
    else:
        # get already done PMIDs
        cur = dbutil.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT pmid FROM pubmed;")
        already_done_pmids = set((r['pmid'] for r in cur.fetchall()))
        log.warning("will skip {} already done... rerun with 'force_update=True' to reclassify all".format(len(already_done_pmids)))




    stats = collections.Counter()

    for idx, ftp_fn in enumerate(ftp_fns):
        local_fn = os.path.join(config.PUBMED_LOCAL_DATA_PATH, os.path.basename(ftp_fn))
        for entry_batch in tqdm.tqdm(grouper(iter_abstracts(local_fn), batch_size), desc="classifying and uploading postgres ({}/{}) {}".format(idx, num_files, local_fn)):



            stats["batches classified"] += 1

            entry_batch = [r for r in entry_batch if r['pmid'] not in already_done_pmids]

            preds = classify(entry_batch)

            for entry, pred in zip(entry_batch, preds):
                if not pred["is_rct_sensitive"]:
                    # ignore anything that isn't an RCT by sensitive criteria at this stage
                    stats["articles excluded"] += 1
                    row = (entry['pmid'], entry['year'], pred['ptyp_rct'], pred['clf_type'], pred['clf_score'])
                    cur = dbutil. db.cursor()
                    cur.execute("INSERT INTO pubmed_excludes (pmid, year, ptyp_rct, clf_type, clf_score) VALUES (%s, %s, %s, %s, %s);", row)
                    cur.close()
                    dbutil.db.commit()
                    continue

                stats["articles included"] += 1

                row = (entry['pmid'], entry['year'], entry['title'], entry['abstract_plaintext'], 
                        json.dumps(entry), ftp_fn, pred['clf_type'], pred['clf_score'], pred['clf_date'], pred['ptyp_rct'], pred['is_rct_precise'],
                        pred['is_rct_balanced'], pred['is_rct_sensitive'])

                # ignore clf_type, clf_score, and clf_date for the moment
                cur = dbutil. db.cursor()
                cur.execute("INSERT INTO pubmed (pmid, year, ti, ab, pm_data, source_filename, clf_type, clf_score, clf_date, ptyp_rct, is_rct_precise, is_rct_balanced, is_rct_sensitive) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", 
                    row)
                cur.close()
                dbutil.db.commit()

    log.info(str(stats))

# def add_ptyp():
#     cur = dbutil. db.cursor()
#     cur.execute("update pubmed set ptyp_rct=(pm_data @> '{\"ptyp\": [\"Randomized Controlled Trial\"]}');")
#     cur.close()
#     dbutil.db.commit()
    




def update():
    download_ftp_baseline()
