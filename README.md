# Youtube Data API Pipelines

Pipelines to fetch comments & video properties from list of videos in a Youtube channel. 
Before running the scripts, make sure of following things:
- List of videos ID to be downloaded must be provided in 'idlist.txt'
- OAuth2 Access to the Youtube channel needed, with SCOPES set to 'youtube-force-ssl'. Place the client's secret to 'client_secret.json'


To get video properties:
run *getvideoprop.py*

To get video comments:
run *getvideocomment.py*
