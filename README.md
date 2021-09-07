# trialstreamer

in progress...

## To get started

1. Install and run postgresql
2. Create a database named 'trialstreamer'
3. make a copy of config.json.bak to config.json, and populate
4. Download RobotReviewer, and follow installation instructions (https://github.com/ijmarshall/robotreviewer)
5. Run RobotReviewer in the REST API mode
6. Run the following code to classify PubMed (typically 20 hours with GPU). 
   This requires a proper setting of `ictrp_retrieval_path`, `pubmed_local_data_path` and `pubmed_user_email` configuration variables.
```
from trialstreamer import pubmed
pubmed.download_ftp_baseline()
```

7. run `python -m trialstreamer` to start the server on localhost:5000

 
## Running with Docker
Create a `.env` file from the `.envTemplate` file provided. Then, fill in the environment variables.
NOTE: If a variable with the prefix `TRIALSTREAMER_` was also defined in the `config.json` file, the environment variable will overwrite it.

```
docker-compose up --build --remove-orphans
```

## Running with Docker in development mode
```
docker-compose -f docker-compose.dev.yml up --build --remove-orphans
```
This mode allows reloading the app when changes are detected.