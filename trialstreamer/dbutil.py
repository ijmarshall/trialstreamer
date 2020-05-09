#
# database utilities
#

from trialstreamer import config
import psycopg2
import psycopg2.extras
import datetime

db = psycopg2.connect(dbname=config.POSTGRES_DB, user=config.POSTGRES_USER,
                      host=config.POSTGRES_IP, password=config.POSTGRES_PASS,
                      port=config.POSTGRES_PORT)


def make_tables():
    """
    set up the database if it doesn't yet exist
    """
    create_tables_command = ("""create table if not exists pubmed (
            id serial primary key,
            pmid varchar(16) unique,
            pm_status varchar(32),
            year integer,
            ti text,
            ab text,
            pm_data jsonb,
            ptyp_rct smallint,
            indexing_method varchar(32),
            is_human boolean,
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
            rct_probability real,
            updated_date timestamp,
            source_filename varchar(256)
            );

            create table if not exists pubmed_excludes (
            id serial primary key,
            pmid varchar(16) unique,
            pm_status varchar(32),
            year integer,
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
            rct_probability real,
            source_filename varchar(256)
            );

create unique index if not exists pubmed_pmid on pubmed (pmid);
create unique index if not exists pubmed_id on pubmed (id);


create table if not exists pubmed_annotations (
    id serial primary key,
    pmid varchar(16) unique,
    population jsonb,
    interventions jsonb,
    outcomes jsonb,
    population_mesh jsonb,
    interventions_mesh jsonb,
    outcomes_mesh jsonb,
    num_randomized integer,
    population_berts float[],
    interventions_berts float[],
    outcomes_berts float[],
    prob_low_bias real,
    punchline_text text,
    effect varchar(22)
);


create index if not exists idx_pubmed_annotations on pubmed_annotations (pmid);

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



create table if not exists ictrp (
            id serial primary key,
            regid varchar(32) unique,
            ti text,
            year integer,
            ictrp_data jsonb,
            url varchar(512),
            population jsonb,
            interventions jsonb,
            outcomes jsonb,
            population_mesh jsonb,
            interventions_mesh jsonb,
            outcomes_mesh jsonb,
            target_size varchar(10),
            is_rct varchar(16),
            is_recruiting varchar(64),
            countries jsonb,
            date_registered timestamp,
            source_filename varchar(256),
            updated_date timestamp
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



create table if not exists registry_links (
            id serial primary key,
            regid varchar(32),
            pmid varchar(16)
            );

create index if not exists idx_registry_pmids on registry_links (pmid);
create index if not exists idx_registry_regids on registry_links (regid);

create table if not exists update_log (
            id serial primary key,
            update_type varchar(16),
            source_filename varchar(256),
            source_date timestamp,
            download_date timestamp,
            update_date timestamp
            );


create index if not exists idx_pmid_dois on pmid_dois (pmid);
create index if not exists idx_pm_data on pubmed using gin((pm_data->'mesh'))
    where is_rct_balanced=true;
create index if not exists idx_ictrp_pop on pubmed_annotations using gin(population_mesh jsonb_path_ops);
create index if not exists idx_ictrp_int on pubmed_annotations using gin(interventions_mesh jsonb_path_ops);
create index if not exists idx_ictrp_out on pubmed_annotations using gin(outcomes_mesh jsonb_path_ops);
create index if not exists idx_pubmed_pop on pubmed_annotations using gin(population_mesh jsonb_path_ops);
create index if not exists idx_pubmed_int on pubmed_annotations using gin(interventions_mesh jsonb_path_ops);
create index if not exists idx_pubmed_out on pubmed_annotations using gin(outcomes_mesh jsonb_path_ops);
create index if not exists idx_ti_vec on pubmed using gin(to_tsvector('english' , ti)) where is_rct_balanced=true;
create index if not exists idx_ti_ab_vec on pubmed using gin(to_tsvector('english', (ti || '  ' || ab))) where is_rct_balanced=true;


create materialized view if not exists pubmed_year_counts AS
    select year,
        sum(case is_rct_precise when true then 1 else 0 end) as is_rct_precise,
        sum(case is_rct_balanced when true then 1 else 0 end) as is_rct_balanced,
        count(*) as is_rct_sensitive,
        sum(case ptyp_rct when 1 then 1 else 0 end) as ptyp_rct,
        round(count(*) * avg(rct_probability)) as est_rct_count
from pubmed where year >= 1948 group by year;

create materialized view if not exists pubmed_rct_count as select count(*) as count_rct_balanced from pubmed where is_rct_balanced=true;


create table if not exists medrxiv_covid19 (
            id serial primary key,
            doi varchar(512),
            url varchar(512),
            year integer,
            date timestamp,
            ti text,
            ab text,
            is_human boolean,
            is_rct_precise boolean,
            is_rct_balanced boolean,
            is_rct_sensitive boolean,
            rct_probability real,
            population jsonb,
            interventions jsonb,
            outcomes jsonb,
            population_mesh jsonb,
            interventions_mesh jsonb,
            outcomes_mesh jsonb,
            authors jsonb,
            source varchar(32),
            num_randomized integer,
            punchline_text text,
            prob_low_bias real,
            effect varchar(22),
            updated_date timestamp
            );

create table if not exists pubmed_bert (
           id serial primary key,
           pmid varchar(16),
           scibert jsonb
);



""")

# """
# create index if not exists idx_medrxiv_data on medrxiv_covid19 using gin(population_mesh) where is_rct_balanced=true;
# create index if not exists idx_medrxiv_data on medrxiv_covid19 using gin(interventions_mesh) where is_rct_balanced=true;
# create index if not exists idx_medrxiv_data on medrxiv_covid19 using gin(outcomes_mesh) where is_rct_balanced=true;
# """
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
