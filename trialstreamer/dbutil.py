#
# database utilities
#

from trialstreamer import config
import psycopg2
import psycopg2.extras
import datetime

db = psycopg2.connect(dbname=config.POSTGRES_DB, user=config.POSTGRES_USER,
                      host='localhost', password=config.POSTGRES_PASS)


def make_tables():
    """
    set up the database if it doesn't yet exist
    """
    create_tables_command = ("""create table if not exists pubmed (
            id serial primary key,
            pmid varchar(16) unique,
            pm_status varchar(32),
            year varchar(8),
            ti text,
            ab text,
            pm_data jsonb,
            ptyp_rct smallint,
            indexing_method varchar(32),
            is_rct_precise boolean,
            is_rct_balanced boolean,
            is_rct_sensitive boolean,
            clf_type varchar(16),
            clf_score real,
            clf_date timestamp,
            score_cnn real,
            score_svm real,
            score_svm_cnn real,
            score_cnn_ptyp real,
            score_svm_ptyp real,
            score_svm_cnn_ptyp real,
            source_filename varchar(256)
            );

create index if not exists idx_is_rct_precise on pubmed (is_rct_precise)
    where is_rct_precise=true;
create index if not exists idx_is_rct_balanced on pubmed (is_rct_balanced)
    where is_rct_balanced=true;
create index if not exists idx_is_rct_sensitive on pubmed (is_rct_sensitive)
    where is_rct_sensitive=true;
create index if not exists idx_pmid on pubmed (pmid) where
    is_rct_balanced=true;
create index if not exists idx_pm_status on pubmed(pm_status)
    where is_rct_balanced=true;

create index if not exists idx_pmid_dois on pmid_dois (pmid);
create index if not exists idx_pm_data on pubmed using gin((pm_data->'mesh'))
    where is_rct_balanced=true;
create index if not exists idx_ti_vec on pubmed using gin(to_tsvector('english'
    , ti)) where is_rct_balanced=true;
create index if not exists idx_ti_ab_vec on pubmed using
    gin(to_tsvector('english', (ti || '  ' || ab))) where is_rct_balanced=true;


create table if not exists ictrp (
            id serial primary key,
            regid varchar(32) not null,
            ti text,
            year varchar(8),
            ictrp_data jsonb,
            source_filename varchar(256)
            );

create table if not exists upw (
            id serial primary key,
            pmid varchar(16),
            is_oa boolean,
            url text,
            url_for_pdf text,
            upw_data jsonb
            );

create table if not exists pmid_dois (
            id serial primary key,
            pmid varchar(16),
            doi varchar(512)
            );

create index if not exists idx_pmid_dois on pmid_dois (pmid);

create table if not exists update_log (
            id serial primary key,
            update_type varchar(16),
            source_filename varchar(256),
            source_date timestamp,
            download_date timestamp
            );





""")
    cur = db.cursor()
    cur.execute(create_tables_command)
    cur.close()
    db.commit()


def log_update(update_type=None, source_filename=None, source_date=None,
               download_date=None):
    if download_date is None:
        download_date = datetime.datetime.now()
    cur = db.cursor()
    cur.execute("INSERT INTO update_log (update_type, source_filename, "
                "source_date, download_date) VALUES (%s, %s, %s, %s);",
                (update_type, source_filename, source_date, download_date))
    cur.close()
    db.commit()


def last_update(update_type):
    """
    return details about last update of specific type
    """
    cur = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT * FROM update_log WHERE update_type=(%s) ORDER BY "
                "source_date DESC LIMIT 1;", (update_type,))
    records = cur.fetchone()
    cur.close()
    if records:
        return dict(records)
    else:
        return None


def update_counts():
    cur = db.cursor()
    cur.execute("SELECT COUNT(*) FROM pubmed WHERE is_rct_precise=true;")


make_tables()  # if they don't exist
