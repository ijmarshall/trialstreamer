# trialstreamer

in progress...

## To get started

1. Install and run postgresql
2. Create a database named 'trialstreamer'
3. make a copy of config.json.bak to config.json, and populate
4. Download RobotSearch (https://github.com/ijmarshall/robotsearch) and ensure is accessible on PYTHONPATH
5. run the following code to classify PubMed (typically 20 hours with GPU)
```
from trialstreamer import pubmed
pubmed.download_ftp_baseline()
```
6. run `python -m trialstreamer` to start the server on localhost:5000

 
