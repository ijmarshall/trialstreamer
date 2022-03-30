import argparse
import datetime
from trialstreamer import pubmed
from trialstreamer.dbutil import db, log_update


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Trialstreamer daily update script')

    parser.add_argument('--source', type=str, help='pubmed|medrxiv')

    args = parser.parse_args()

    print("""
    Welcome to the TrialStreamer daily update script
    NB Please this will only work if RobotReviewer is running locally using the API mode
    """)

    # FIRST DO UPDATES
    # NB! RobotReviewer MUST be running locally in API mode (ideally on a GPU)

    if not args.source:
        print("Missing --source argument")
        parser.print_help()
    elif args.source == 'pubmed':
        print("Downloading any updates from PubMed")
        pubmed.download_ftp_updates()
        print("Annotating using RobotReviewer")
        pubmed.annotate_rcts()
        print("Updating counts")
        pubmed.update_counts()
        print("Updating logs")
        log_update(update_type="fullcheck", download_date=datetime.datetime.utcnow())
        print("Done! :)")
    elif args.source == 'medrxiv':
        print("Updating MedRxiv COVID-19 articles")
        from trialstreamer import medrxiv_cov
        medrxiv_cov.update()
        print("Updating logs")
        log_update(update_type="medrxiv", download_date=datetime.datetime.utcnow())
    else:
        print("Invalid --source argument, must be one of the following: (pubmed|medrxiv)")
        parser.print_help()
