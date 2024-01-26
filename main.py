import logging
import time
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import json
from datetime import datetime
import datetime
import sqlite3
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os
from flask import Flask, request, url_for, session, redirect
from dotenv import load_dotenv
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
scheduler = BackgroundScheduler()

logging.basicConfig(filename='app.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

app.config['SESSION_COOKIE_NAME'] = 'Spotify Cookie'

load_dotenv()

DATABASE_LOCATION = os.getenv('DATABASE_LOCATION')
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
TOKEN_INFO = os.getenv('TOKEN_INFO')
logger.info('fuck me')
app.secret_key = os.getenv('SECRET_KEY')

def my_cron_job():
  try:
    # Your scheduled task logic
    logger.info('Scheduler task executed.')
  except Exception as e:
    print(f'Error in scheduler task: {e}', exc_info=True)

def check_if_valid_date(df: pd.DataFrame) -> bool:
  if df.empty:
    print("No songs have been downloaded")
    return False
  logger.info('logger is working')
  if pd.Series(df['played_at']).is_unique:
    pass
  else:
    raise Exception("Primary Key Check is violated")

  if df.isnull().values.any():
    raise Exception("Null value found")

  return True

@app.route('/')
def login():
  auth_url = create_spotify_oauth().get_authorize_url()
  return redirect(auth_url)

@app.route('/redirect')
def redirect_page():

  session.clear()
  code = request.args.get('code')
  token_info = create_spotify_oauth().get_access_token(code)
  session[TOKEN_INFO] = token_info

  return redirect(url_for('save_recently_played', external=True))

@app.route('/checkdatabase')
def checkdata():
  engine = sqlalchemy.create_engine(DATABASE_LOCATION)
  conn = sqlite3.connect('my_tracks_2.sqlite')
  stuff = pd.read_sql('select * from my_tracks_2', conn)
  print(stuff['played_at'])
  
  return 'stuff'

@app.route('/saveRecentlyPlayed')
def save_recently_played():
  print('I have a request')
  try:
    token_info = get_token()
  except:
    print("User not logged in")
    return redirect('/')
  
  sp = spotipy.Spotify(auth=token_info['access_token'])

  today = datetime.datetime.now()
  yesterday = today - datetime.timedelta(days=10)
  yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
  recently_played = sp.current_user_recently_played(limit=50, after=yesterday_unix_timestamp)
  song_dict = get_track_list_df(recently_played)
  song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])
  
  if not check_if_valid_date(song_df):
    return "false"
  
  engine = sqlalchemy.create_engine(DATABASE_LOCATION)
  conn = sqlite3.connect('my_tracks_2.sqlite')
  cursor = conn.cursor()

  sql_query = """
  CREATE TABLE IF NOT EXISTS my_tracks_2(
    song_name VARCHAR(200),
    artist_name VARCHAR(200),
    played_at VARCHAR(200),
    timestamp DATETIME,
    CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
  )"""
  
  cursor.execute(sql_query)
  print("Opened database successfully")

  try:
    song_df.to_sql('my_tracks_2', con=conn, index=False, if_exists='append')
  except Exception as e:
    print("Data already exists in the database. Error: "+ str(e))
  
  conn.close()
  print("Closed database successfully")

  return "success"

def get_track_list_df(recently_played):
  song_names = []
  artist_names = []
  played_at = []
  played_at_list = []
  timestamps = []

  for song in recently_played["items"]:
    song_names.append(song["track"]["name"])
    artist_names.append(song["track"]["album"]["artists"][0]["name"])
    played_at = datetime.datetime.strptime(song["played_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
    played_at_list.append(played_at)
    timestamps.append(song["played_at"][0:10])
  
  song_dict = {
    "song_name": song_names,
    "artist_name": artist_names,
    "played_at": played_at_list,
    "timestamp": timestamps
  }

  return song_dict

def get_token():
  token_info = session.get(TOKEN_INFO, None)
  if not token_info:
    redirect(url_for('login', external=False))

  now = int(time.time())

  is_expired = token_info['expires_at'] - now < 60
  if (is_expired):
    spotify_oath = create_spotify_oauth()
    token_info = spotify_oath.refresh_access_token(token_info['refresh_token'])
  return token_info

def create_spotify_oauth():
  return SpotifyOAuth(
    client_id = CLIENT_ID,
    client_secret=CLIENT_SECRET,
    redirect_uri=url_for('redirect_page', _external=True),
    scope= 'user-read-recently-played'
    )

scheduler.add_job(
  func=my_cron_job,
  trigger=CronTrigger(hour="*", minute="*", start_date=datetime.datetime.now())
)
print(datetime.datetime.now())
if __name__ == '__main__':
  scheduler.start()

  app.run(debug=False)