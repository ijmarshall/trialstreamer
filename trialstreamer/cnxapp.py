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
from connexion.exceptions import OAuthProblem
from flask_cors import CORS
from flask import send_file

with open(os.path.join(trialstreamer.DATA_ROOT, 'rct_model_calibration.json'), 'r') as f:
    clf_cutoffs = json.load(f)

with open(os.path.join(trialstreamer.DATA_ROOT, 'pico_mesh_autocompleter.pck'), 'rb') as f:
    pico_trie = pickle.load(f)


def auth(api_key, required_scopes):
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
    if substr is None:
        return jsonify([])

    matches = pico_trie.itervalues(prefix=substr)

    if len(substr) < min_char:
        # for short ones just return first 5
        return jsonify([r for _, r in zip(range(max_return), matches)])
    else:
        # where we have enough chars, process and get top ranked
        return jsonify(sorted(matches, key=lambda x: x['count'], reverse=True)[:max_return])


def picosearch(body):
    """
    gets brief display info for articles matching a structured PICO query
    """
    query = body['terms']

    if len(query)==0:
        return jsonify([])
    retmode = body.get("retmode", "json-short")
    print(retmode)

    builder = []

    for c in query:
        field = sql.SQL('.').join((sql.Identifier("pa"), sql.Identifier(f"{c['field']}_mesh")))
        contents = sql.Literal(Json([{"mesh_ui": c['mesh_ui']}])                           )
        builder.append(sql.SQL(' @> ').join((field, contents)))

    params = sql.SQL(' AND ').join(builder)
    if retmode=='json-short':
        select = sql.SQL("SELECT pm.pmid, pm.ti, pm.year FROM pubmed as pm, pubmed_annotations as pa WHERE ")
    elif retmode=='ris':
        select = sql.SQL("SELECT pm.pmid as pmid, pm.year as year, pm.ti as ti, pm.ab as ab, pm.pm_data->>'journal' as journal FROM pubmed as pm, pubmed_annotations as pa WHERE ")
    join = sql.SQL("AND pm.pmid = pa.pmid AND pm.is_rct_precise=true")

    out = []

    with psycopg2.connect(dbname=trialstreamer.config.POSTGRES_DB, user=trialstreamer.config.POSTGRES_USER,
           host=trialstreamer.config.POSTGRES_IP, password=trialstreamer.config.POSTGRES_PASS,
           port=trialstreamer.config.POSTGRES_PORT) as db:
        with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor, name="pico_mesh") as cur:
            cur.execute(select + params + join)
            for i, row in enumerate(cur):
                if retmode=='json-short':
                    out.append({"pmid": row['pmid'], "ti": row['ti'], "year": row['year']})
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
