import urllib 

import logging
logging.basicConfig(level="INFO", format='[%(levelname)s] %(name)s %(asctime)s: %(message)s')
log = logging.getLogger(__name__)
            
log.info("Welcome to the Trialstreamer Server!")

import trialstreamer
from trialstreamer import ris
from collections import OrderedDict
import datetime
import os
import humanize
from flask import json
import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json
from collections import defaultdict
import pickle
from io import BytesIO as StringIO  # py3
import connexion
import networkx as nx
from connexion.exceptions import OAuthProblem
from flask_cors import CORS
from flask import send_file
from trialstreamer import schwartz_hearst

log.info("Connecting to database")
from trialstreamer import dbutil
log.info('Done!')

log.info("Loading data")
log.info("RCT model calibration")
with open(os.path.join(trialstreamer.DATA_ROOT, 'rct_model_calibration.json'), 'r') as f:
    clf_cutoffs = json.load(f)
log.info("done!")

log.info("Autocompeter")
with open(os.path.join(trialstreamer.DATA_ROOT, 'pico_cui_autocompleter.pck'), 'rb') as f:
    pico_trie = pickle.load(f)
log.info("done!")
# with open(os.path.join(trialstreamer.DATA_ROOT, 'drugs_from_class.pck'), 'rb') as f:
#     drugs_from_class = pickle.load(f)
#  RxNorm should solve this one in subtrees (to test more)
log.info("Metathesaurus trees")
with open(os.path.join(trialstreamer.DATA_ROOT, 'cui_subtrees.pck'), 'rb') as f:
    subtrees = pickle.load(f)
log.info("done!")
def get_subtree(cui):
    try:        
        decs = nx.descendants(subtrees, cui)
    except nx.exception.NetworkXError:
        decs = set()
    decs.add(cui)
    # if mesh_ui in drugs_from_class:
    #     decs.update(drugs_from_class[mesh_ui])
    return decs

def auth(api_key, required_scopes):
    print(trialstreamer.config.API_KEYS)
    print(api_key)
    info = trialstreamer.config.API_KEYS.get(api_key, None)
    if not info:
        raise OAuthProblem('Invalid token')
    return info

def autocomplete(q):
    """
    retrieves most likely MeSH PICO terms for the demo
    """

    min_char = 3
    max_return = 5
    substr = q
    if substr is None or not pico_trie.has_subtrie(substr):
        return []

    matches = pico_trie.itervalues(prefix=substr)

    def flat_list(l):
        return [item for sublist in l for item in sublist]

    def dedupe(l):
        encountered = set()
        out = []
        for r in l:
            if r['cui_pico_display'] not in encountered:
                encountered.add(r['cui_pico_display'])
                out.append(r)
        return out

    if len(substr) < min_char:
        # for short ones just return first 5
        return dedupe(flat_list([r for _, r in zip(range(max_return), matches)]))
    else:
        # where we have enough chars, process and get top ranked
        return sorted(dedupe(flat_list(matches)), key=lambda x: x['count'], reverse=True)[:max_return]

def meta():
    """
    returns last updated date, and also total RCT count
    """

    with psycopg2.connect(dbname=trialstreamer.config.POSTGRES_DB, user=trialstreamer.config.POSTGRES_USER,
           host=trialstreamer.config.POSTGRES_IP, password=trialstreamer.config.POSTGRES_PASS,
           port=trialstreamer.config.POSTGRES_PORT) as db:
        cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # get last PubMed updated date
        cur.execute("select source_date from update_log where update_type='pubmed_update' order by source_date desc limit 1;")
        last_updated = cur.fetchone()['source_date']

        cur.execute("select count_rct_precise from pubmed_rct_count;")
        num_rcts = cur.fetchone()['count_rct_precise']

    return {"last_updated": last_updated, "num_rcts": f'{num_rcts:,}'}


def covid19():
    """
    returns RCTs from Pubmed and MedRxiv in people with Covid-19
    """
    ts_sql = """
    SELECT pm.pmid, pm.ti, pm.year, pa.punchline_text, pa.population, pa.interventions, pa.outcomes,
    pa.population_mesh, pa.interventions_mesh, pa.outcomes_mesh, pa.num_randomized, pa.low_rsg_bias, pa.low_ac_bias,
    pa.low_bpp_bias, pa.punchline_text FROM pubmed as pm, pubmed_annotations as pa WHERE pm.is_rct_precise=true and
    pa.population_mesh@>'[{"mesh_ui": "C000657245"}]' and pm.pmid=pa.pmid;
    """

    medrxiv_sql = """
    SELECT ti, ab, year, punchline_text, population, interventions, outcomes,
    population_mesh, interventions_mesh, outcomes_mesh, num_randomized, low_rsg_bias, low_ac_bias,
    low_bpp_bias, punchline_text FROM medrxiv_covid19 WHERE is_rct_precise=true;
    """

    out = []
    with psycopg2.connect(dbname=trialstreamer.config.POSTGRES_DB, user=trialstreamer.config.POSTGRES_USER,
           host=trialstreamer.config.POSTGRES_IP, password=trialstreamer.config.POSTGRES_PASS,
           port=trialstreamer.config.POSTGRES_PORT) as db:
        cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        out = {}
        cur.execute(ts_sql)
        out['trialstreamer_published'] = [dict(r) for r in cur.fetchall()]
        cur.execute(medrxiv_sql)
        out['trialstreamer_preprint'] = [dict(r) for r in cur.fetchall()]
    return out

def get_cite(authors, journal, year):
    if len(authors) >= 1:
        return f"{authors[0]['LastName']}{' et al.' if len(authors) > 1 else ''}, {journal}. {year}"
    else:
        return f"{journal}. {year}"

def get_medrxiv_cite(authors, source, year):
    return f"{authors[0]['author_name']}{' et al.' if len(authors) > 1 else ''}, {source}. {year}"


def picosearch(body):
    """
    gets brief display info for articles matching a structured PICO query
    """
    query = body['terms']
    
    expand_terms = body.get("expand_terms", True)

    if len(query)==0:
        return []
    retmode = body.get("retmode", "json-short")

    builder = []

    for c in query:
        
        if expand_terms:
            expansion = get_subtree(c['cui']) 
        else:
            expansion = [c['cui']]
                
        subtree_builder = []
        
        for c_i in expansion:
            
            field = sql.SQL('.').join((sql.Identifier("pa"), sql.Identifier(f"{c['field']}_mesh")))                                                                        
            contents = sql.Literal(Json([{"cui": c_i}])                           )
            subtree_builder.append(sql.SQL(' @> ').join((field, contents)))
                                                                                                                                                                
        builder.append(sql.SQL('(') + sql.SQL(' OR ').join(subtree_builder) + sql.SQL(')'))
    
    params = sql.SQL(' AND ').join(builder)
                                                                                                                                                    
    if retmode=='json-short':
        select = sql.SQL("SELECT pm.pmid, pm.ti, pm.ab, pm.year, pa.punchline_text, pa.population, pa.interventions, pa.outcomes, pa.population_mesh, pa.interventions_mesh, pa.outcomes_mesh, pa.num_randomized, pa.low_rsg_bias, pa.low_ac_bias, pa.low_bpp_bias, pa.punchline_text, pm.pm_data->'authors' as authors, pm.pm_data->'journal' as journal, pm.pm_data->'dois' as dois FROM pubmed as pm, pubmed_annotations as pa WHERE ")
    elif retmode=='ris':
        select = sql.SQL("SELECT pm.pmid as pmid, pm.year as year, pm.ti as ti, pm.ab as ab, pm.pm_data->>'journal' as journal FROM pubmed as pm, pubmed_annotations as pa WHERE ")
    join = sql.SQL("AND pm.pmid = pa.pmid AND pm.is_rct_balanced=true limit 250;")
                                                                            
    out = []


    # PUBMED
    with psycopg2.connect(dbname=trialstreamer.config.POSTGRES_DB, user=trialstreamer.config.POSTGRES_USER,
           host=trialstreamer.config.POSTGRES_IP, password=trialstreamer.config.POSTGRES_PASS,
           port=trialstreamer.config.POSTGRES_PORT) as db:
        with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor, name="pico_cui") as cur:
            cur.execute(select + params + join)
            print((select + params + join).as_string(cur))
            for i, row in enumerate(cur):
                if retmode=='json-short':
                    out.append({"pmid": row['pmid'], "ti": row['ti'], "year": row['year'], "punchline_text": row['punchline_text'],
                        "citation": get_cite(row['authors'], row['journal'], row['year']),
                        "population": row['population'],
                        "interventions": row['interventions'],
                        "outcomes": row['outcomes'],
                        "dois": row['dois'],
                        "population_mesh": row['population_mesh'],
                        "interventions_mesh": row['interventions_mesh'],
                        "outcomes_mesh": row['outcomes_mesh'],
                        "low_rsg_bias": row['low_rsg_bias'],
                        "low_ac_bias": row['low_ac_bias'],
                        "low_bpp_bias": row['low_bpp_bias'],
                        "num_randomized": row['num_randomized'],
                        "abbrev_dict": schwartz_hearst.extract_abbreviation_definition_pairs(doc_text=row['ab']),
                        "article_type": "journal article"})
                elif retmode=='ris':
                    out.append(OrderedDict([("TY", "JOUR"),
                                            ("DB", "Trialstreamer"),
                                            ("ID", row['pmid']),
                                            ("TI", row['ti']),
                                            ("YR", row['year']),
                                            ("JO", row['journal']),
                                            ("AB", row['ab'])]))


    ### ICTRP
    if retmode=='json-short':
        ictrp_select = sql.SQL("SELECT pa.regid, pa.ti, pa.year, pa.population, pa.interventions, pa.outcomes, pa.population_mesh, pa.interventions_mesh, pa.outcomes_mesh, pa.target_size, pa.is_rct, pa.is_recruiting, pa.countries, pa.date_registered FROM ictrp as pa WHERE ")
    elif retmode=='ris':
        ictrp_select = sql.SQL("SELECT pa.regid as id, pa.year as year, pa.ti as ti FROM ictrp as pa WHERE ")
    ictrp_join = sql.SQL("AND pa.is_rct='RCT' LIMIT 250;")                                                                            

    with psycopg2.connect(dbname=trialstreamer.config.POSTGRES_DB, user=trialstreamer.config.POSTGRES_USER,
           host=trialstreamer.config.POSTGRES_IP, password=trialstreamer.config.POSTGRES_PASS,
           port=trialstreamer.config.POSTGRES_PORT) as db:
        with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor, name="pico_mesh") as cur:
            cur.execute(ictrp_select + params + ictrp_join)
            print((ictrp_select + params + ictrp_join).as_string(cur))
            for i, row in enumerate(cur):
                if retmode=='json-short':
                    out_d = dict(row)
                    out_d['article_type']="trial registration"
                    out.append(out_d)
                elif retmode=='ris':
                    # TODO MAKE RIS REASONABLE FOR ICTRP
                    pass




    ### START COVID-19 PREPRINTS
    if any(((q_i['cui']=="TS-COV19") and (q_i['field']=="population") for q_i in query)):

        if retmode=='json-short':
            cov_select = sql.SQL("SELECT pa.ti, pa.ab, pa.year, pa.punchline_text, pa.population, pa.interventions, pa.outcomes, pa.population_mesh, pa.interventions_mesh, pa.outcomes_mesh, pa.num_randomized, pa.low_rsg_bias, pa.low_ac_bias, pa.low_bpp_bias, pa.punchline_text, pa.authors, pa.source, pa.doi FROM medrxiv_covid19 as pa WHERE ")
        elif retmode=='ris':
            cov_select = sql.SQL("SELECT pa.year as year, pa.ti as ti, pa.ab as ab FROM medrxiv_covid19 as pa WHERE ")
        cov_join = sql.SQL(" AND pa.is_rct_precise=true AND pa.is_human=true LIMIT 250;")

                                                                            

        with psycopg2.connect(dbname=trialstreamer.config.POSTGRES_DB, user=trialstreamer.config.POSTGRES_USER,
           host=trialstreamer.config.POSTGRES_IP, password=trialstreamer.config.POSTGRES_PASS,
           port=trialstreamer.config.POSTGRES_PORT) as db:
            with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor, name="pico_mesh") as cur:
                cur.execute(cov_select + params + cov_join)
                print((cov_select + cov_join).as_string(cur))
                for i, row in enumerate(cur):
                    if retmode=='json-short':
                        out.append({"ti": row['ti'], "year": row['year'], "punchline_text": row['punchline_text'],
                            "citation": get_medrxiv_cite(row['authors'], row['source'], row['year']),
                            "population": row['population'],
                            "interventions": row['interventions'],
                            "dois": [row['doi']],
                            "outcomes": row['outcomes'],
                            "population_mesh": row['population_mesh'],
                            "interventions_mesh": row['interventions_mesh'],
                            "outcomes_mesh": row['outcomes_mesh'],
                            "low_rsg_bias": row['low_rsg_bias'],
                            "low_ac_bias": row['low_ac_bias'],
                            "low_bpp_bias": row['low_bpp_bias'],
                            "num_randomized": row['num_randomized'],
                            "abbrev_dict": schwartz_hearst.extract_abbreviation_definition_pairs(doc_text=row['ab']),
                            "article_type": "preprint"})
                    elif retmode=='ris':
                        pass
                        # TODO MAKE RIS REASONABLE FOR ICTRP



    if retmode=='json-short':
        return out
    elif retmode=='ris':
        report = ris.dumps(out)
        strIO = StringIO()
        strIO.write(report.encode('utf-8')) # need to send as a bytestring
        strIO.seek(0)
        return send_file(strIO,
                         attachment_filename="trialstreamer.ris",
                         as_attachment=True)


def get_trial(uuid):
    print(uuid)
    
    out = []
    with psycopg2.connect(dbname=trialstreamer.config.POSTGRES_DB, user=trialstreamer.config.POSTGRES_USER,
               host=trialstreamer.config.POSTGRES_IP, password=trialstreamer.config.POSTGRES_PASS,
               port=trialstreamer.config.POSTGRES_PORT) as db:
        with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # first try pubmid @TODO probably we can ascertain this w/a regular expression and avoid this
            # brute force means of checking if it's a pmid.
            # @TODO this flow is in general pretty terrible; we should really explicitly flag
            # which table/source the requested uuid (or again, infer based on the str)
            # instead of this terrible nested series of attempts. 
            # byron is to blame.
            select = sql.SQL("""
                SELECT pm.pmid, pm.ti, pm.ab, pm.year, pa.punchline_text, pa.population, pa.interventions, pa.outcomes, 
                pa.population_mesh, pa.interventions_mesh, pa.outcomes_mesh, pa.num_randomized, pa.low_rsg_bias, 
                pa.low_ac_bias, pa.low_bpp_bias, pa.punchline_text, pm.pm_data->'authors' as authors, pm.pm_data->'journal' as journal, 
                pm.pm_data->'dois' as dois FROM pubmed as pm, pubmed_annotations as pa 
                WHERE (pm.pmid = '{0}' AND pa.pmid = '{0}')""".format(uuid))
            cur.execute(select)

            if cur.rowcount > 0:
                # then we found a trial in the pubmed table
                for i, row in enumerate(cur):
                    out.append({"pmid": row['pmid'], "ti": row['ti'], "year": row['year'], "punchline_text": row['punchline_text'],
                        "citation": get_cite(row['authors'], row['journal'], row['year']),
                        "population": row['population'],
                        "interventions": row['interventions'],
                        "outcomes": row['outcomes'],
                        "dois": row['dois'],
                        "population_mesh": row['population_mesh'],
                        "interventions_mesh": row['interventions_mesh'],
                        "outcomes_mesh": row['outcomes_mesh'],
                        "low_rsg_bias": row['low_rsg_bias'],
                        "low_ac_bias": row['low_ac_bias'],
                        "low_bpp_bias": row['low_bpp_bias'],
                        "num_randomized": row['num_randomized'],
                        "abbrev_dict": schwartz_hearst.extract_abbreviation_definition_pairs(doc_text=row['ab']),
                        "article_type": "journal article"})
                return out
            

            # didn't find it; try ICTRP
            ictrp_select = sql.SQL("""
                SELECT pa.regid, pa.ti, pa.year, pa.population, pa.interventions, pa.outcomes, pa.population_mesh, 
                pa.interventions_mesh, pa.outcomes_mesh, pa.target_size, pa.is_rct, pa.is_recruiting, pa.countries, 
                pa.date_registered FROM ictrp as pa WHERE pa.regid = '{0}'""".format(uuid))
            cur.execute(ictrp_select)
            if cur.rowcount > 0:
                for i, row in enumerate(cur):                    
                    out_d = dict(row)
                    out_d['article_type']="trial registration"
                    out.append(out_d)
                return out 

            # finally, resort to medarxiv
            # I was unable to get swagger to cooperate with allowing even *escaped* fwd slashes
            # -- it just would not route them here. 
            # uuid = urllib.parse.unquote(uuid) # because DOIs contain fwd slashes that need to be escaped
            #
            # For now, I have done the terrible thing of assuming we have swapped them with `-`. sorry.
            uuid = uuid.replace("-", "/")
            med_arxiv_select = sql.SQL("""SELECT ti, ab, year, punchline_text, population, interventions, outcomes, population_mesh, interventions_mesh, outcomes_mesh, num_randomized, low_rsg_bias, low_ac_bias, low_bpp_bias, punchline_text FROM medrxiv_covid19 WHERE doi='{0}'""".format(uuid))
            
            cur.execute(med_arxiv_select)
            
            if cur.rowcount > 0:
                for i, row in enumerate(cur):  
                    out_d = dict(row)
                    out_d['article_type']="preprint"
                    out.append(out_d)
                return out

    # if we fail to find this uuid anywhere, return an empty list
    return out
    


import connexion
app = connexion.FlaskApp(__name__, specification_dir='api/', port=trialstreamer.config.TS_PORT, server='gevent')
app.add_api('trialstreamer_api.yml')
CORS(app.app)
