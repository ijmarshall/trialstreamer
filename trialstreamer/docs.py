# trialstreamer docs interface for robotdata
# uses RobotReviewer classifications for generating PubMed RCT dataset, so
# fewer missed
#

from trialstreamer import dbutil
import psycopg2
from robotdata import docs


def iter_pubmed(threshold='balanced'):

    cur = dbutil.db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("select * from pubmed where is_rct_{}=true;".format(threshold))
    records = cur.fetchall()
    for record in records:
        yield docs.PubmedArticle(record['pmid'], record['data'])
