#
#   MedRxiv - Covid RCT downloads
#

import trialstreamer
import os
import json
import requests
import datetime
from trialstreamer import config, dbutil
import psycopg2
import time
import logging

log = logging.getLogger(__name__)


with open(os.path.join(trialstreamer.DATA_ROOT, 'rct_model_calibration.json'), 'r') as f:
    clf_cutoffs = json.load(f)

def get_articles():
    url = "https://connect.medrxiv.org/relate/collection_json.php?grp=181"
    feed = requests.get(url)
    feed_parsed = json.loads(feed.text)    
    articles = []
    meta = []

    for a in feed_parsed['rels']:
        articles.append({"ti": a['rel_title'], "ab": a['rel_abs']})
        meta.append({"date": a['rel_date'], "doi": a['rel_doi'], "url": a['rel_link'],
                         "year": datetime.datetime.strptime(a['rel_date'], "%Y-%m-%d").year,
                    "authors": a['rel_authors'], "source": a["rel_site"]})

    # add any key manually added papers for now from the local json
    with open(os.path.join(trialstreamer.DATA_ROOT, 'manual_preprints.json'), 'r') as f:
        extras = json.load(f)

    for e in extras:
        articles.append({k: e[k] for k in ['ti', 'ab']})
        meta.append({k: e[k] for k in ['date', 'doi', 'url', 'year', 'authors', 'source']})

    return {"articles": articles, "meta": meta}


def predict(X, tasks=None, filter_rcts="is_rct_sensitive"):

    if tasks is None:
        tasks = ['rct_bot']
    base_url = config.ROBOTREVIEWER_URL
    upload_data = {
        "articles": X,
        "robots": tasks,
        "filter_rcts": filter_rcts
    }
    r = requests.post(base_url+'queue-documents', json=upload_data, headers={"api-key": trialstreamer.config.ROBOTREVIEWER_API_KEY})
    report_id = r.json()

    def check_report(report_id):
        r = requests.get(base_url +'report-status/'+report_id['report_id'], headers={"api-key": trialstreamer.config.ROBOTREVIEWER_API_KEY})
        return r.json()['state'] == 'SUCCESS'

    while not check_report(report_id):
        time.sleep(0.3)

    report = requests.get(base_url +'report/'+report_id['report_id'], headers={"api-key": trialstreamer.config.ROBOTREVIEWER_API_KEY}).json()
    return report


def upload_to_postgres(annotations, meta):
    """
    we will keep those meeting the sensitive threshold only for RCTs
    """
    cur = dbutil.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    # given small numbers, will start again
    cur.execute('delete from medrxiv_covid19;')
    
    for a, m in zip(annotations, meta):
        
        if not a['rct_bot']['is_rct_sensitive']:
            # skip the non RCTs
            continue
        sample_size = a.get('sample_size_bot', {}).get('num_randomized')
        if sample_size == 'not found' or int(sample_size) > 1000000:
            sample_size = None

        cur.execute("""INSERT INTO medrxiv_covid19 (doi, url, year, date, 
        ti, ab, is_human, is_rct_precise, is_rct_balanced, is_rct_sensitive,
        rct_probability, population, interventions, outcomes, population_mesh,
        interventions_mesh, outcomes_mesh, num_randomized, prob_low_rob,
        punchline_text, effect, authors, source) VALUES (%s, 
        %s, %s, %s, %s, %s, %s,  %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s);
        """, (m['doi'], m['url'], m['year'], m['date'], a['ti'], a['ab'],
             a['human_bot']['is_human'], a['rct_bot']['is_rct_precise'], 
             a['rct_bot']['is_rct_balanced'], a['rct_bot']['is_rct_sensitive'],
             a['rct_bot']["preds"]["probability"],
             json.dumps(a['pico_span_bot']['population']),
             json.dumps(a['pico_span_bot']['interventions']),
             json.dumps(a['pico_span_bot']['outcomes']),
             json.dumps(a['pico_span_bot']['population_mesh']),
             json.dumps(a['pico_span_bot']['interventions_mesh']),
             json.dumps(a['pico_span_bot']['outcomes_mesh']),
             sample_size,
             a['bias_ab_bot']['prob_low_rob'],
             a['punchline_bot']['punchline_text'],
             a['punchline_bot']['effect'],
            json.dumps(m['authors']),
             m['source']))
   
    
def update():
    log.info("Fetching articles from MedRxiv feed")
    articles = get_articles()
    log.info("Annotating articles with RobotReviewer")
    annotations = predict(articles['articles'], tasks=['rct_bot', 'human_bot', 'pico_span_bot', 'sample_size_bot', 'bias_ab_bot', 'punchline_bot'], filter_rcts="is_rct_sensitive")
    log.info("Uploading to DB")
    upload_to_postgres(annotations, articles['meta'])    
    dbutil.db.commit()
    log.info("All done!")
