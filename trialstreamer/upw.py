#
#   parse unpaywall data
#


import logging
log = logging.getLogger(__name__)

from trialstreamer import config
from trialstreamer import dbutil
import psycopg2
import tqdm
import trialstreamer
import gzip
import glob
import os
import json


def get_pmid_doi_lookup(limit_to='is_rct_balanced'):


    pmid_from_doi = {}

    log.info('looking up DOIs from pubmed')
    cur = dbutil.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("select pmid, pm_data->>'dois' from pubmed where {}=true;".format(limit_to))
    records = cur.fetchall()

    for pmid, dois_str in tqdm.tqdm(records, desc="parsing pubmed local database for dois..."):
        dois = json.loads(dois_str)
        if dois:
            pmid_from_doi[dois[0]] = pmid



    log.info('looking up DOIs from mendeley (local data store)')
    cur.execute("select pmid_dois.pmid, pmid_dois.doi from pmid_dois, pubmed where (pmid_dois.pmid = pubmed.pmid) and (pubmed.{}=true);".format(limit_to))

    records = cur.fetchall()

    for r in tqdm.tqdm(records, desc="parsing pubmed local database (mendeley links) for dois..."):


        if r['pmid'] not in pmid_from_doi and r['doi'] and len(r['doi'])>12:
            # dois < 12 chars probably are errors...
            pmid_from_doi[r['doi']] = r['pmid']

    return pmid_from_doi






def parse_db_dump(force_update=False, limit_to='is_rct_balanced'):
    if force_update==False:
        log.error('This will delete all data UPW data in the database and start again. Please run with force_update=True to do this.')
        return 0

    if limit_to not in ['is_rct_balanced', 'is_rct_sensitive', 'is_rct_precise']:
        raise Exception('limit_to not recognised, needs to be "is_rct_precise", "is_rct_balanced", or "is_rct_sensitive"')

    if limit_to == 'is_rct_sensitive':
        log.warning("Getting metadata for all articles with sensitive cutoff... May take a long time...")

    log.info('deleting all full text links from table...')
    cur = dbutil.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute('delete from upw;')

    log.info("looking up dois to parse...")
    pmid_from_doi_lookup = get_pmid_doi_lookup(limit_to=limit_to)

    pth = os.path.join(trialstreamer.DATA_ROOT, 'upw', '*.*')
    fns = glob.glob(pth)



    with gzip.open(fns[0], 'r') as f:
        for i, l in tqdm.tqdm(enumerate(f), desc="parsing lines from upw database dump..."):

            d = json.loads(l)
            if d['doi'] in pmid_from_doi_lookup and d['is_oa']:

                row = (pmid_from_doi_lookup[d['doi']], d['is_oa'], d['best_oa_location'].get('url'), d['best_oa_location'].get('url_for_pdf'), json.dumps(d))
                cur.execute('insert into upw (pmid, is_oa, url, url_for_pdf, upw_data) values (%s, %s, %s, %s, %s);', row)

    dbutil.db.commit()








