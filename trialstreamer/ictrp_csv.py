#
#   Temporary ICTRP download updater, works with CSV file, not as nicely processed as Gert's version for now
#


from trialstreamer import dbutil, config
import trialstreamer
import psycopg2
import psycopg2.extras
import logging
import os
import re
import datetime
import tqdm
from robotreviewer.textprocessing import minimap
import sys
from parse import *
import json
import zipfile
import csv
import glob
import io




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


headers = [str(r) for r in range(60)]

headers[0] = "study_id"
headers[4] = "scientific_title"
headers[5] = "url"
headers[29] = "health_conditions"#, AD (; sep)
headers[30] = "interventions"#, AE (; sep)
headers[33] = "primary_outcome"#, AH (primary outcome), AI (secondary outcomes)
headers[34] = "secondary_outcomes"
headers[19] = "study_design"
headers[24] = "recruitment_status"
headers[23] = "target_size"
headers[21] = "date_registered"
headers[28] = "countries"

dateRegisteredFormat = {
    'ANZCTR': '%d/%m/%Y',
    'DRKS': '%d/%m/%Y',
    'EUCTR': '%d/%m/%Y',
    'ISRCTN': '%d/%m/%Y',
    'JPRN': '%d/%m/%Y',
    'NCT': '%d/%m/%Y',
    'NTR': '%d/%m/%Y',
    'PACTR': '%d/%m/%Y',
    'REBEC': '%d/%m/%Y',
    'RPCEC': '%d/%m/%Y',
    'TCTR': '%d/%m/%Y',
    'ChiCTR': '%Y-%m-%d',
    'CRIS': '%Y-%m-%d',
    'IRCT': '%Y-%m-%d',
    'SLCTR': '%Y-%m-%d',
    'CTRI': '%d-%m-%Y',
    'REPEC': '%d/%m/%Y',
    'UNK': "%Y-%m-%d"
}

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



# def is_recruiting(recruitment_status):
#     if recruitment_status == "Recruiting":
#         return "recruiting"
#     elif recruitment_status=="Not Recruiting":
#         return "not recruiting"
#     else:
#         return "unknown"

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
def cleanup(raw):
    txt = re.sub("[^a-zA-Z\d]", " ", raw)
    txt = re.sub("\s\s+", " ", txt)
    return txt


def guess_registry(raw):
    try:
        return re.search("[A-Z]+", raw)[0]
    except:
        return "UNK"


def parsenull(s, default='unknown'):
        if s=='NULL':
                return default
        else:
                return s

def parse_ictrp(ictrp_data):

    out = {"regid": ictrp_data['study_id']}
    out["ti"] = parsenull(ictrp_data['scientific_title']).strip()


    out["population"] = [r.strip() for r in parsenull(ictrp_data["health_conditions"]).split(';')]
    out["interventions"] = [r.strip() for r in parsenull(ictrp_data["interventions"]).split(';')]


    out['outcomes'] = []
    if ictrp_data["primary_outcome"]!='NULL':
        out["outcomes"] = [r.strip() for r in ictrp_data["primary_outcome"].split(';')]
    if ictrp_data["secondary_outcomes"]!='NULL':
        out["outcomes"] += [r.strip() for r in ictrp_data["secondary_outcomes"].split(';')]

    out["is_rct"] = is_rct(ictrp_data.get('study_design'))

    out["is_recruiting"] = parsenull(ictrp_data["recruitment_status"]).lower()

    try:
        out["target_size"] = str(int(ictrp_data['target_size']))
        if len(out["target_size"]) > 10:
            out['target_size'] = "unknown"
    except:
        out['target_size'] = "unknown"

    try:
        out["date_registered"] = datetime.datetime.strptime(ictrp_data['date_registered'],dateRegisteredFormat[guess_registry(ictrp_data['study_id'])])
    except:
        out["date_registered"] = None

    try:
        out['year'] = out['date_registered'].year
    except:
        out['year'] = None

    try:
        out["countries"] = list(set(ictrp_data['countries'].split(';')))
    except:
        out["countries"] = []

    for f in ['population', 'interventions', 'outcomes']:
        try:
            out[f"{f}_mesh"] = minimap.get_unique_terms((cleanup(o_i) for o_i in out[f] if o_i))
        except:
            out[f"{f}_mesh"] = []

    out['url'] = ictrp_data.get('url')

    return out


def check_if_new_data():

    # first find out our most recent filename and date
    lu = dbutil.last_update("ictrp")
    fns = glob.glob(os.path.join(config.ICTRP_DATA_PATH, '*.zip'))
    fn_d = [{"fn": fn, "date": parse("ICTRPFullExport-{:d}-{:tg}.zip", os.path.basename(fn))[1]} for fn in fns]

    most_recent_fn = sorted(fn_d, key=lambda x: (x['date']), reverse=True)[0]

    if lu is None or (lu['source_date'] < most_recent_fn['date']):
        log.info('New update available')
        return most_recent_fn

    else:
        log.info('No new updates available')
        return None


def upload_to_postgres(fn, force_update=False):


    cur = dbutil.db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if force_update:
        cur.execute("DELETE FROM ictrp;")
        already_done = set()
    else:
        cur.execute("SELECT regid from ictrp;")
        already_done = set((r['regid'] for r in cur))

    with zipfile.ZipFile(fn) as zipf:
            csv_fn = [i.filename for i in zipf.infolist() if os.path.splitext(i.filename)[-1]=='.csv'][0]
            readme_fn = [i.filename for i in zipf.infolist() if os.path.splitext(i.filename)[-1]=='.txt'][0]

            with io.TextIOWrapper(zipf.open(csv_fn), encoding="utf-8-sig") as csvf:

                reader = csv.DictReader(csvf, fieldnames=headers, delimiter=",")
                for i, r in tqdm.tqdm(enumerate(reader), desc="parsing ICTRP entries"):

                        if i % 500 == 0:
                            dbutil.db.commit()

                        if r['study_id'] in already_done:
                            continue

                        if is_rct(r.get('study_design'))!='RCT':
                                continue

                        p = parse_ictrp(r)
                        row = (p['regid'], p['ti'], json.dumps(p['population']), json.dumps(p['interventions']),
                            json.dumps(p['outcomes']), json.dumps(p['population_mesh']),
                            json.dumps(p['interventions_mesh']), json.dumps(p['outcomes_mesh']),
                            p['is_rct'], p['is_recruiting'], p['target_size'], p['date_registered'], p['year'],
                            json.dumps(p['countries']), json.dumps([]), fn, p['url']) # temporarily we will not have the full parsed data

                        cur.execute("INSERT INTO ictrp (regid, ti, population, interventions, outcomes, population_mesh, interventions_mesh, outcomes_mesh, is_rct, is_recruiting, target_size, date_registered, year, countries, ictrp_data, source_filename, url) VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                            row)

                        already_done.add(r['study_id'])

    cur.close()
    dbutil.db.commit()


def update(force_update=False):
    log.info('checking for latest data')
    fn = check_if_new_data()
    if fn is None:
        log.info('no new data found')
        return None
    log.info('found new file {}... processing'.format(fn['fn']))
    download_date = datetime.datetime.now()
    log.info('Parsing to postgres {}'.format(fn['fn']))
    upload_to_postgres(fn['fn'], force_update=force_update)
    dbutil.log_update(update_type='ictrp', source_filename=fn['fn'], source_date=fn['date'], download_date=download_date)


