#!/usr/bin/env python

import csv
import os
import pickle
import json
import pandas as pd
from datetime import datetime as dt
 
import google.oauth2.credentials
 
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
 
# The CLIENT_SECRETS_FILE variable specifies the name of a file that contains
# the OAuth 2.0 information for this application, including its client_id and
# client_secret.
CLIENT_SECRETS_FILE = "client_secret.json"
 
# This OAuth 2.0 access scope allows for full read/write access to the
# authenticated user's account and requires requests to use an SSL connection.
SCOPES = ['https://www.googleapis.com/auth/youtube.force-ssl']
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'

with open('./idlist.txt') as f:
    idlist = [line.rstrip().rstrip('\n').rstrip('\r') for line in f]
# lineList = [line.rstrip('\n') for line in open(fileName)]
# IDLIST = [ 'JXeKATf42zE', 'L-c1tApTJ2g'] 
 
 
def get_authenticated_service():
    credentials = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            credentials = pickle.load(token)
    #  Check if the credentials are invalid or do not exist
    if not credentials or not credentials.valid:
        # Check if the credentials have expired
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CLIENT_SECRETS_FILE, SCOPES)
            credentials = flow.run_console()
 
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(credentials, token)
 
    return build(API_SERVICE_NAME, API_VERSION, credentials = credentials)
 

def get_channel_videoid(service, **kwargs):
    idlist = []
    id_data = service.channels().list(**kwargs).execute()
    


def get_video_data(service, **kwargs):
    video_metadata = []
    video_data = service.videos().list(**kwargs).execute()
    for item in video_data['items']:
        title = item['snippet']['title'].encode("utf-8")
        url = 'https://www.youtube.com/watch?v={}'
        date_ori = item['snippet']['publishedAt']
        date = dt.strptime(date_ori, "%Y-%m-%dT%H:%M:%S.%fZ")
        video_metadata.append([title, url.format(kwargs['id']), date])
    return video_metadata
 
 
def compile_video_data(service, **kwargs):
    final_video_data = []
    for single_id in idlist :
        video_data = get_video_data(service, part='snippet', id= single_id)
        final_video_data.extend(video_data)
    title_reference = pd.DataFrame.from_records(final_video_data, columns=["title", "url", "date"])
    title_reference.to_csv(path_or_buf='./title_reference.csv', index=False)
    
    # return { 'title' : title_reference, 'idlist': idlist }


 
    # write_to_csv(final_result)

 
if __name__ == '__main__':
    # When running locally, disable OAuthlib's HTTPs verification. When
    # running in production *do not* leave this option enabled.
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    service = get_authenticated_service()
    compile_video_data(service, part='snippet,items')