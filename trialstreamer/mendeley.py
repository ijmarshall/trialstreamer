"""
Get Mendeley metadata from PubMed articles or CDSR articles
"""

import logging

from mendeley import Mendeley
from mendeley.exception import MendeleyException
from trialstreamer import config
from urllib.parse import urlparse
from trialstreamer import dbutil
import psycopg2
import tqdm
import re
from bs4 import BeautifulSoup


log = logging.getLogger(__name__)


def doi_clean(raw):
    """
    preprocessor for any doi from mendeley
    """

    if raw.startswith('<a '):

        elem = BeautifulSoup(raw, 'html.parser')
        raw = elem.find('a').get('href', "")

    if raw.startswith('http'):
        raw = urlparse(raw).path[1:]  # (i.e. remove the leading `/`)
    # if doesn't start with 10. now then see if we can front strip
    if not raw.startswith('10.'):
        try:
            raw = re.findall(r'10.\d{4,9}/[-._;()/:A-Za-z0-9]+', raw)[0]
        except IndexError:
            raw = None
    return raw


def get_mendeley_session():
    mendeley = Mendeley(config.MENDELEY_ID, config.MENDELEY_SECRET)
    return mendeley.start_client_credentials_flow().authenticate()


def get_mendeley_metadata(force_refresh=False, limit_to='is_rct_balanced'):
    """
    get mendeley metadata for all PubMed content
    """

    if limit_to not in ['is_rct_balanced', 'is_rct_sensitive',
                        'is_rct_precise']:
        raise Exception('limit_to not recognised, needs to be '
                        '"is_rct_precise","is_rct_balanced", or '
                        '"is_rct_sensitive"')

    if limit_to == 'is_rct_sensitive':
        log.warning("Getting metadata for all articles with sensitive cutoff.."
                    "May take a long time...")

    # since otherwise get *way* too much log
    logging.getLogger("requests_oauthlib").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    cur = dbutil.db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if force_refresh:
        # deleting doi table from database
        log.info('deleting all dois from table...')
        cur.execute('delete from pmid_dois;')

    log.info('finding PubMed RCTs without DOIs')
    cur.execute("select pmid from pubmed where {}=true "
                "and pm_data->>'dois'='[]';".format(limit_to))
    records = cur.fetchall()
    pmids_todo = set((r['pmid'] for r in records))
    log.info('{} records without DOIs'.format(len(pmids_todo)))

    log.info('finding already done')
    cur.execute("select pmid from pmid_dois;")
    records = cur.fetchall()
    pmids_done = set((r['pmid'] for r in records))
    log.info('{} records already done'.format(len(pmids_done)))

    pmids_todo = pmids_todo - pmids_done
    log.info('{} records still to lookup'.format(len(pmids_todo)))

    mendeley = get_mendeley_session()

    for pmid in tqdm.tqdm(pmids_todo):
        try:
            links = mendeley.catalog.by_identifier(pmid=pmid).identifiers

            if 'doi' in links:
                doi = doi_clean(links['doi'])
            else:
                doi = None
        except MendeleyException:
            # unable to find doc
            doi = None
        cur.execute('insert into pmid_dois (pmid, doi) values (%s, %s);',
                    (pmid, doi))
        dbutil.db.commit()

    cur.close()
    if force_refresh:
        dbutil.log_update(update_type='doi_full')
    else:
        dbutil.log_update(update_type='doi_update')
