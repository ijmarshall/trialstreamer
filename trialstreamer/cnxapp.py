from trialstreamer import dbutil
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
from flask import jsonify
from io import BytesIO as StringIO  # py3
import connexion
import networkx as nx
from connexion.exceptions import OAuthProblem
from flask_cors import CORS
from flask import send_file
from trialstreamer import schwartz_hearst
            

with open(os.path.join(trialstreamer.DATA_ROOT, 'rct_model_calibration.json'), 'r') as f:
    clf_cutoffs = json.load(f)

with open(os.path.join(trialstreamer.DATA_ROOT, 'pico_mesh_autocompleter.pck'), 'rb') as f:
    pico_trie = pickle.load(f)

with open(os.path.join(trialstreamer.DATA_ROOT, 'mesh_subtrees.pck'), 'rb') as f:
    subtrees = pickle.load(f)

def get_subtree(mesh_ui):
    try:        
        decs = nx.descendants(subtrees, mesh_ui)
    except nx.exception.NetworkXError:
        return set([mesh_ui])
    decs.add(mesh_ui)
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
        return jsonify([])

    matches = pico_trie.itervalues(prefix=substr)

    def flat_list(l):
        return [item for sublist in l for item in sublist]

    def dedupe(l):
        encountered = set()
        out = []
        for r in l:
            if r['mesh_pico_display'] not in encountered:
                encountered.add(r['mesh_pico_display'])
                out.append(r)
        return out

    if len(substr) < min_char:
        # for short ones just return first 5
        return jsonify(dedupe(flat_list([r for _, r in zip(range(max_return), matches)])))
    else:
        # where we have enough chars, process and get top ranked
        return jsonify(sorted(dedupe(flat_list(matches)), key=lambda x: x['count'], reverse=True)[:max_return])

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

    return jsonify({"last_updated": last_updated, "num_rcts": f'{num_rcts:,}'})


def covid19():
    """
    returns RCTs from Pubmed and Medarxiv in people with Covid-19
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
    low_bpp_bias, punchline_text FROM medarxiv_covid19 WHERE is_rct_precise=true;
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
    return jsonify(out)

def get_cite(authors, journal, year):
    if len(authors) >= 1:
        return f"{authors[0]['LastName']}{' et al.' if len(authors) > 1 else ''}, {journal}. {year}"
    else:
        return f"{journal}. {year}"


def picosearch(body):
    """
    gets brief display info for articles matching a structured PICO query
    """
    query = body['terms']
    
    expand_terms = body.get("expand_terms", True)

    if len(query)==0:
        return jsonify([])
    retmode = body.get("retmode", "json-short")

    builder = []

    for c in query:
        
        if expand_terms:
            expansion = get_subtree(c['mesh_ui']) 
        else:
            expansion = [c['mesh_ui']]
                
        subtree_builder = []
        
        for c_i in expansion:
            
            field = sql.SQL('.').join((sql.Identifier("pa"), sql.Identifier(f"{c['field']}_mesh")))                                                                        
            contents = sql.Literal(Json([{"mesh_ui": c_i}])                           )
            subtree_builder.append(sql.SQL(' @> ').join((field, contents)))
                                                                                                                                                                
        builder.append(sql.SQL('(') + sql.SQL(' OR ').join(subtree_builder) + sql.SQL(')'))
    
    params = sql.SQL(' AND ').join(builder)
                                                                                                                                                    
    if retmode=='json-short':
        select = sql.SQL("SELECT pm.pmid, pm.ti, pm.ab, pm.year, pa.punchline_text, pa.population, pa.interventions, pa.outcomes, pa.population_mesh, pa.interventions_mesh, pa.outcomes_mesh, pa.num_randomized, pa.low_rsg_bias, pa.low_ac_bias, pa.low_bpp_bias, pa.punchline_text, pm.pm_data->'authors' as authors, pm.pm_data->'journal' as journal FROM pubmed as pm, pubmed_annotations as pa WHERE ")
    elif retmode=='ris':
        select = sql.SQL("SELECT pm.pmid as pmid, pm.year as year, pm.ti as ti, pm.ab as ab, pm.pm_data->>'journal' as journal FROM pubmed as pm, pubmed_annotations as pa WHERE ")
    join = sql.SQL("AND pm.pmid = pa.pmid AND pm.is_rct_precise=true AND pm.is_human=true LIMIT 250;")
                                                                            
    out = []



    with psycopg2.connect(dbname=trialstreamer.config.POSTGRES_DB, user=trialstreamer.config.POSTGRES_USER,
           host=trialstreamer.config.POSTGRES_IP, password=trialstreamer.config.POSTGRES_PASS,
           port=trialstreamer.config.POSTGRES_PORT) as db:
        with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor, name="pico_mesh") as cur:
            cur.execute(select + params + join)
            print((select + params + join).as_string(cur))
            for i, row in enumerate(cur):
                if retmode=='json-short':
                    out.append({"pmid": row['pmid'], "ti": row['ti'], "year": row['year'], "punchline_text": row['punchline_text'],
                        "citation": get_cite(row['authors'], row['journal'], row['year']),
                        "population": row['population'],
                        "interventions": row['interventions'],
                        "outcomes": row['outcomes'],
                        "population_mesh": row['population_mesh'],
                        "interventions_mesh": row['interventions_mesh'],
                        "outcomes_mesh": row['outcomes_mesh'],
                        "low_rsg_bias": row['low_rsg_bias'],
                        "low_ac_bias": row['low_ac_bias'],
                        "low_bpp_bias": row['low_bpp_bias'],
                        "num_randomized": row['num_randomized'],
                        "abbrev_dict": schwartz_hearst.extract_abbreviation_definition_pairs(doc_text=row['ab'])})
                elif retmode=='ris':
                    out.append(OrderedDict([("TY", "JOUR"),
                                            ("DB", "Trialstreamer"),
                                            ("ID", row['pmid']),
                                            ("TI", row['ti']),
                                            ("YR", row['year']),
                                            ("JO", row['journal']),
                                            ("AB", row['ab'])]))

    if retmode=='json-short':
        return jsonify(out)
    elif retmode=='ris':
        report = ris.dumps(out)
        strIO = StringIO()
        strIO.write(report.encode('utf-8')) # need to send as a bytestring
        strIO.seek(0)
        return send_file(strIO,
                         attachment_filename="trialstreamer.ris",
                         as_attachment=True)



import connexion
app = connexion.FlaskApp(__name__, specification_dir='api/', port=trialstreamer.config.TS_PORT, server='gevent')
app.add_api('trialstreamer_api.yml')
CORS(app.app)
