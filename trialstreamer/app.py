from flask import Flask, render_template
from flask import request
from trialstreamer import dbutil
import trialstreamer
import datetime
import os
import humanize
import json
import psycopg2
from collections import defaultdict
from flask import jsonify

app = Flask(__name__)




@app.route('/')
def hello_world():
    return 'trialstreamer :)'


@app.route('/status')
def status():
    """
    Retrieves most recent update dates
    """
    update_types = ["ictrp", "pubmed_baseline", "pubmed-daily"]

    out = []

    for t in update_types:
        lu = dbutil.last_update(t)
        if lu:
            lu['age'] = humanize.naturaldelta(datetime.datetime.now() - lu['source_date'])
            out.append(lu)

    return render_template('status.html', results=out)


@app.route('/rcts')
def rcts():
    """
    Displays info about RCTs in database
    """

    # select count(*) from pubmed where pm_data @> '{"ptyp": ["Randomized Controlled Trial"]}';


    threshold_types = ["precise", "balanced", "sensitive"]
    threshold_colors = ['#e41a1c','#377eb8','#4daf4a','#984ea3','#ff7f00','#ffff33','#a65628']
    threshold_fills = ['#fbb4ae','#b3cde3','#ccebc5','#decbe4','#fed9a6','#ffffcc','#e5d8bd']
    threshold_shapes = ['circle', 'star', 'triangle', 'line', 'crossRot','circle', 'circle']

    global clf_cutoffs

    cur = dbutil.db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    totals = []

    breakdowns = defaultdict(dict)



    for t in threshold_types:

        cur.execute("select count(*) from pubmed where is_rct_{}=true;".format(t))
        record = cur.fetchone()
        totals.append({"count": record['count'], "threshold_type": t})

        cur.execute("select year, count(*) from pubmed where is_rct_{}=true group by year;".format(t))
        records = cur.fetchall()
        for r in records:
            breakdowns[r['year']][t] = r['count']

        cur.execute("select year, count(*) from pubmed where (clf_type='svm_cnn' and is_rct_{}=true) group by year;".format(t))
        records = cur.fetchall()
        for r in records:
            breakdowns[r['year']]["{} no ptyp".format(t)] = r['count']

    
    cur.execute("select count(*), year from pubmed where ptyp_rct=1 group by year;")
    records = cur.fetchall()
    for r in records:
        breakdowns[r['year']]["PubMed PT tag"] = r['count']

    
    cur.execute("select count(*), year from ictrp group by year;")
    records = cur.fetchall()
    for r in records:
        breakdowns[r['year']]["Trial registries"] = r['count']


    breakdowns.pop('')
    breakdowns.pop(None)

    # for now remove blank years, but there are a few thousand in that group

    # breakdowns_ptyp.pop('')
    # breakdowns_ptyp.pop(None)

    
    year_labels = sorted(breakdowns.keys())
    datasets = []

    print(year_labels)

    for t, tc, ts, tf in zip(threshold_types + ["{} no ptyp".format(t) for t in threshold_types] + ["PubMed PT tag", "Trial registries"], threshold_colors, threshold_shapes, threshold_fills):
        print(t)
        datasets.append({"label": t, "data": [breakdowns[y].get(t, 0) for y in year_labels], "fill": False, "borderColor": tc, "pointStyle": ts, "borderWidth": 2, "backgroundColor": tf})


    cur.close()
    return render_template('rcts.html', totals=totals, labels=json.dumps(year_labels), datasets=json.dumps(datasets))







@app.route('/db_dump')
def db_dump():
    """
    dumps the full database
    TODO figure out whether to do as a pgdump, or as
    a custom standardised data format
    """
    return 'db_dump placeholder'

@app.route('/query')
def query():
    """
    emulated PubMed-style boolean query
    """
    # still to improve!!
    # currently searches ti/ab text only, using default text search
    # no indexes yet

    q = request.args.get('q', '')
    threshold_type = request.args.get('threshold_type', 'balanced')
    start = int(request.args.get('start', 0))
    end = int(request.args.get('end', start+10))
    cur = dbutil.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT pm_data FROM pubmed WHERE (to_tsvector(ti || '  ' || ab) @@ to_tsquery(%s)) limit %s offset %s;", (q, end-start, int(start)))
    records = cur.fetchall()

    return jsonify(records)


@app.route('/get')
def get():
    """
    returns record or records matching id or list of ids
    by tsid, pmid, or regid params
    """
    return 'get placeholder'


@app.route('/fuzzy')
def fuzzy():
    """
    fuzzy matcher
    pass some combination of identifiable article parms
    e.g. title, pdf md5 hash, journal, citation fields

    returns single match only if high certainty, else none
    """
    return 'fuzzy matcher placeholder'



def main():
    debug = os.environ.get('APP_DEBUG', True)
    host = os.environ.get('APP_HOST', '0.0.0.0')
    port = int(os.environ.get('APP_PORT', 5000))
    app.run(debug=debug, host=host, port=port)


if __name__ == '__main__':
    main()
