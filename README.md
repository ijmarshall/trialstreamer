# Trialstreamer

in progress...

## To get started

1. Install and run postgresql
2. Create a database named 'trialstreamer'
3. Make a copy of config.json.example to config.json, and populate
4. Download RobotReviewer, and follow installation instructions (https://github.com/ijmarshall/robotreviewer)
5. Run RobotReviewer in the REST API mode
6. Run the following code to classify PubMed (typically 20 hours with GPU). 
   This requires a proper setting of `ictrp_retrieval_path`, `pubmed_local_data_path` and `pubmed_user_email` configuration variables.
   
```
from trialstreamer import pubmed
pubmed.download_ftp_baseline()
```

7. run `python -m trialstreamer` to start the server on `localhost:5000`

 
## Running with Docker

Create a `.env` file from the `.envTemplate` file provided. Then, fill in the environment variables.
NOTE: If a variable with the prefix `TRIALSTREAMER_` was also defined in the `config.json` file, the environment variable will overwrite it.
Make sure the `pubmed_local_data_path` configured at `config.json` exists and is the same as the one used as the volumes
specified in docker compose files. It is recommended that this path is not located under `trialstreamer` folder.

To start the Trialstreamer API and Updates Crontab services, run the following commands after RobotReviewer is already running:

```
docker-compose build
docker-compose up -d 
```
or, as a single command:
```
docker-compose up --build -d 
```

If RobotReviewer is not running with docker-compose, you will have to create the network `robotreviewer_default` with:
```
docker network create robotreviewer_default 
```

## Running with Docker in development mode

```
docker-compose -f docker-compose.dev.yml up --build -d
```

The development mode exposes the port 5000 and allows reloading the app when changes are detected, but the update script is not run automatically. 
However, the update script can be ran directly in the API docker container if necessary using the following command: 

```
docker exec -ti trialstreamer_api_1 python update.py --source=<pubmed|medrxiv>
```
