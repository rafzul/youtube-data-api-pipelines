#!/usr/bin/env python
from __future__ import unicode_literals
from csv import reader
import os
import pickle
import pandas as pd
import sqlite3
from datetime import datetime as dt
import json
import sys
 
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

# logs = open("./logs.out", 'w')
# sys.stdout = logs

with open('./idlist.txt') as f:
    idlist = [line.rstrip().rstrip('\n').rstrip('\r') for line in f]

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

def insert_to_sqlite(comment_id, text_comment, author_comment, comment_hierarchy, parent_id, time_comment, likecount_comment, title, url, replies):
    try:
        #connect to sqlite
        conn = sqlite3.connect('hookspace_ytdata.db')
        conn.text_factory = str
        c = conn.cursor()
        #query input
        input_data = (comment_id, text_comment, author_comment, comment_hierarchy, parent_id, time_comment, likecount_comment, title, url, replies)
        query = """INSERT INTO comments (comment_id, text, author, hierarchy, parent, time, like_count, video_title, video_url, replies) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        #query for insertion 
        c.execute(query, input_data)
        conn.commit()
        print("Sqlite record inserted ", c.rowcount)
    
    except sqlite3.Error as error:
        print("Sqlite record insertion failed", error)

def get_video_data(single_id):
    with open('./title_reference.csv', 'r') as readobject:
        csv_reader = reader(readobject)
        videosprop = list(csv_reader)
    url = 'https://www.youtube.com/watch?v={}'.format(single_id)
    for items in videosprop:
        if url in items[1]:
            title_date_url = [items[0], items[1], items[2]]
    return title_date_url


def get_video_comments(service, single_id, **kwargs):
    #get title data
    titleurl = get_video_data(single_id)
    video_title = titleurl[0]
    video_url = titleurl[1]
    video_date = titleurl[2]
    #fetching comments from youtube api, systematically adding them to sqlite database
    comments = []
    emptydefault = ['none', 'none', 'none', 'none', 'none', video_date, 0, video_title, video_url, 'none']
    results = service.commentThreads().list(**kwargs).execute()
    if results['items']:
        while results:
            for item in results['items']:
                hierarchy = 'parent'
                parent = item['snippet']['topLevelComment']['id'].encode('utf-8', 'ignore')
                comment_id = item['snippet']['topLevelComment']['id'].encode('utf-8', 'ignore')
                text = item['snippet']['topLevelComment']['snippet']['textDisplay'].encode('utf-8', 'ignore')
                time_ori = item['snippet']['topLevelComment']['snippet']['publishedAt']
                time = dt.strptime(time_ori, "%Y-%m-%dT%H:%M:%S.%fZ")
                author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'].encode('utf-8', 'ignore')
                like_count = item['snippet']['topLevelComment']['snippet']['likeCount']
                replies_old = item.get('replies','none')
                replies = json.dumps(replies_old)
                #insert to sqlite as 
                insert_to_sqlite(comment_id, text, author, hierarchy, parent, time, like_count, video_title, video_url, replies)
                #append data to list COMMENTS
                comments.append([comment_id, text, author, hierarchy, parent, time, like_count, video_title, video_url, replies]) 

            # Check if another page exists
            if 'nextPageToken' in results:
                kwargs['pageToken'] = results['nextPageToken']
                results = service.commentThreads().list(**kwargs).execute()
            else:
                break
    else:
            insert_to_sqlite(emptydefault[0], emptydefault[1], emptydefault[2], emptydefault[3], emptydefault[4], emptydefault[5], emptydefault[6], emptydefault[7], emptydefault[8], emptydefault[9])
            comments.append([emptydefault[0], emptydefault[1], emptydefault[2], emptydefault[3], emptydefault[4], emptydefault[5], emptydefault[6], emptydefault[7], emptydefault[8], emptydefault[9]])
    return comments

def compile_comment_and_videoprop(service, **kwargs):
    final_data = []
    #open video properties
    for single_id in idlist :
        #comments = get_video_comments(service, single_id, part='snippet,replies,id', videoId= single_id, textFormat='plainText', 
        comments = get_video_comments(service, single_id, part='snippet, replies', videoId= single_id, textFormat='plainText', maxResults='100')
        final_data.extend(comments)
        print('Comment for https://www.youtube.com/watch?v={} added!').format(single_id)
    #turn final_data into dataframe
    final_data_pd = pd.DataFrame.from_records(final_data, columns=["comment_id","text","author", "hierarchy", "parent", "time","like_count", "video_title", "video_url", "replies"])
    # write_to_csv(final_result)
    final_data_pd.to_csv(path_or_buf='./comments.csv', index=False)

    # logs.close()
    

if __name__ == '__main__':
    # When running locally, disable OAuthlib's HTTPs verification. When
    # running in production *do not* leave this option enabled.
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    service = get_authenticated_service()
    compile_comment_and_videoprop(service, part='snippet, replies', maxResults='100')