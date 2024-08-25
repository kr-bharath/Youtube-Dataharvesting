# ["Youtube API libraries"]
from googleapiclient.discovery import build
import re

# [SQL libraries]
import mysql.connector
import sqlalchemy
from googleapiclient.errors import HttpError
from sqlalchemy import create_engine
import pymysql
import pandas as pd

# [STREAMLIT libraries]
from streamlit_option_menu import option_menu
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="YouTube", layout="wide", initial_sidebar_state="expanded")

# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["Home","Data Zone","Analysis Zone","Query Zone"],
                                 default_index=0,
                                 orientation="vertical",
                                 styles={"nav-link": {"font-size": "24px", "text-align": "center", "margin": "0px",
                                                      "--hover-color": "#C80101"},
                                         "icon": {"font-size": "30px"},
                                         "container": {"max-width": "6000px"},
                                         "nav-link-selected": {"background-color": "#C80101"}})

# HOME PAGE
if selected == "Home":
    # Title
    st.title('[YOUTUBE DATA HARVESTING and WAREHOUSING using SQL and STREAMLIT]')
    st.markdown("## [Domain] : Social Media")
    st.markdown("## [Skills take away From This Project] : Python scripting, Data Collection, Streamlit, API integration, Data Management using SQL")
    st.markdown("## [Overall view] : Building a simple UI with Streamlit, retrieving data from YouTube API, storing the date SQL as a WH, querying the data warehouse with SQL, and displaying the data in the Streamlit app.")
    st.markdown("## [Developed by] : Revathi Karthikeyan")

# Data Zone
elif selected == "Data Zone":
    col1, col2 = st.columns(2)
    with col1:
        st.markdown('## [Data collection zone]')
        st.write('(**collects data** by using channel id and **stores it in the [SQL] database**.)')
        channel_id = st.text_input('**Enter the channel_id**')
        st.write('''click below to retrieve and store data.''')
        Get_data = st.button('**Retrieve and store data**')

        # Define Session state to Get data button
        if "Get_state" not in st.session_state:
            st.session_state.Get_state = False
        if Get_data or st.session_state.Get_state:
            st.session_state.Get_state = True

            api_key = "AIzaSyCvFdJYH_NOJNHA92x9qFVs1D_HhnqrM50"
            youtube = build("youtube", "v3", developerKey=api_key)

            # Function to get channel statistics
            try:
                def get_channel_stats(channel_id):
                    all_data = []

                    request = youtube.channels().list(
                        part="snippet,contentDetails,statistics",
                        id=channel_id)
                    response = request.execute()

                    for i in range(len(response['items'])):
                        data = dict(channel_name=response['items'][i]['snippet']['title'],
                                    channel_id=response['items'][i]['id'],
                                    subscribers=response['items'][i]['statistics']['subscriberCount'],
                                    views=response['items'][i]['statistics']['viewCount'],
                                    Total_videos=response['items'][i]['statistics']['videoCount'],
                                    channel_description=response['items'][i]['snippet']['description'],
                                    Playlist_Id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])
                        all_data.append(data)

                    return all_data

                get_channel_stats(channel_id)

                channel_data = pd.DataFrame(get_channel_stats(channel_id))

                channel_data['subscribers'] = pd.to_numeric(channel_data['subscribers'])
                channel_data['views'] = pd.to_numeric(channel_data['views'])
                channel_data['Total_videos'] = pd.to_numeric(channel_data['Total_videos'])

                # playlist information
                def get_pl_info(youtube, channel_id):
                    next_page_token = None
                    playlist = []

                    while True:
                        request = youtube.playlists().list(
                            part='snippet,contentDetails',
                            channelId=channel_id,
                            maxResults=50)
                        response = request.execute()

                        for i in range(len(response['items'])):
                            data = dict(pl_id=response['items'][i]['id'],
                                        channel_name=response['items'][i]['snippet']['title'],
                                        channel_id=response['items'][i]['snippet']['channelId'],
                                        publish_at=response['items'][i]['snippet']['publishedAt'],
                                        videos_count=response['items'][i]['contentDetails']['itemCount'])
                            playlist.append(data)
                        next_page_token = response.get('nextPageToken')
                        if next_page_token is None:
                            break
                    return playlist

                pl_data = pd.DataFrame(get_pl_info(youtube, channel_id))

                # Get VIDEO IDs
                def get_video_ids(channel_id):

                    request = youtube.channels().list(
                        part="snippet,contentDetails,statistics",
                        id=channel_id, )
                    response = request.execute()
                    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

                    video_ids = []
                    next_page_token = None

                    while True:
                        request = youtube.playlistItems().list(
                            part='snippet',
                            playlistId=Playlist_Id,
                            maxResults=50,
                            pageToken=next_page_token)
                        response1 = request.execute()

                        for i in range(len(response1['items'])):
                            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
                        next_page_token = response1.get('nextPageToken')

                        if next_page_token is None:
                            break
                    return video_ids

                video_ids = get_video_ids(channel_id)

                # Function to get video details
                def get_video_details(video_ids):
                    video_stats = []

                    for i in range(0, len(video_ids), 50):
                        response = youtube.videos().list(
                            part="snippet,contentDetails,statistics",
                            id=','.join(video_ids[i:i + 50])).execute()
                        for video in response['items']:
                            video_details = dict(Channel_name=video['snippet']['channelTitle'],
                                                 Channel_id=video['snippet']['channelId'],
                                                 Video_id=video['id'],
                                                 Title=video['snippet']['title'],
                                                 Tags=video['snippet'].get('tags'),
                                                 Thumbnail=video['snippet']['thumbnails']['default']['url'],
                                                 Description=video['snippet']['description'],
                                                 Published_date=video['snippet'].get('publishedAt'),
                                                 Duration=video['contentDetails']['duration'],
                                                 Views=video['statistics']['viewCount'],
                                                 Likes=video['statistics'].get('likeCount'),
                                                 Comments=video['statistics'].get('commentCount'),
                                                 Favorite_count=video['statistics']['favoriteCount'],
                                                 Caption_status=video['contentDetails']['caption'])
                            video_stats.append(video_details)

                    return video_stats

                v_stats = get_video_details(video_ids)
                v_stats_df = pd.DataFrame(v_stats)

                v_stats_df['Duration'] = pd.to_timedelta(v_stats_df['Duration'])

                # Convert timedelta to string format
                v_stats_df['Duration'] = v_stats_df['Duration'].astype(str)

                # Format the string to hh:mm:ss
                v_stats_df['Duration'] = v_stats_df['Duration'].str.extract(r'(\d+:\d+:\d+)')

                v_stats_df['Published_date'] = pd.to_datetime(v_stats_df['Published_date']).dt.date
                v_stats_df['Views'] = pd.to_numeric(v_stats_df['Views'])
                v_stats_df['Likes'] = pd.to_numeric(v_stats_df['Likes'])
                v_stats_df['Comments'] = pd.to_numeric(v_stats_df['Comments'])
                v_stats_df['Tags'] = v_stats_df['Tags'].astype(str)

                def convert_duration(Duration):
                    regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
                    match = re.match(regex, 'Duration')
                    if not match:
                        return '00:00:00'
                    hours, minutes, seconds = match.groups()
                    hours = int(hours[:-1]) if hours else 0
                    minutes = int(minutes[:-1]) if minutes else 0
                    seconds = int(seconds[:-1]) if seconds else 0
                    total_seconds = hours * 3600 + minutes * 60 + seconds

                    return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds / 3600), int((total_seconds % 3600) / 60),
                                                          int(total_seconds % 60))

                # Get COMMENT INFORMATION

                def get_comment_info(video_ids):
                    Comment_data = []
                    try:
                        for video_id in video_ids:
                            request = youtube.commentThreads().list(
                                part="snippet",
                                videoId=video_id,
                                maxResults=50)

                            response = request.execute()

                            for item in response['items']:
                                data = dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                                            Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                                            Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                            Comment_Author=item['snippet']['topLevelComment']['snippet'][
                                                'authorDisplayName'],
                                            Comment_Published=item['snippet']['topLevelComment']['snippet'][
                                                'publishedAt'])

                                Comment_data.append(data)
                    except:
                        pass
                    return Comment_data

                comment_data = get_comment_info(video_ids)
                channel_comment = pd.DataFrame(comment_data)
            except:
                pass

    with col2:
        st.markdown('## [Data Migration zone]')
        st.write('''( **Migrates channel data to [MYSQL] database**)''')

        st.write('''Click below for **Data migration**.''')
        Migrate = st.button('**Migrate to MySQL**')
        if 'migrate_sql' not in st.session_state:
            st.session_state.migrate_sql = False
        if Migrate or st.session_state.migrate_sql:
            st.session_state.migrate_sql = True

            # **Connection to SQL**
            connect = pymysql.connect(host="localhost",
                                      user="root",
                                      password="051092",
                                      database="youtubedata",
                                      port=3306)  # Corrected port number
            mycursor = connect.cursor()
            mycursor.execute('CREATE DATABASE IF NOT EXISTS youtubedata')
            mycursor.close()
            connect.close()

            engine = create_engine("mysql+pymysql://root:051092@localhost:3306/youtubedata")
            connection = engine.connect()

            channel_data.to_sql(con=engine, name='channels', if_exists='append', index=False,
                                dtype={'Channel_Name': sqlalchemy.types.VARCHAR(length=225),
                                       'Channel_Id': sqlalchemy.types.VARCHAR(length=225),
                                       'Subscribers': sqlalchemy.types.BigInteger,
                                       'Views': sqlalchemy.types.INT,
                                       'Total_Videos': sqlalchemy.types.BigInteger,
                                       'Channel_Description': sqlalchemy.types.TEXT,
                                       'Playlist_Id': sqlalchemy.types.VARCHAR(length=225)})

            pl_data.to_sql(con=engine, name='playlist', if_exists='append', index=False,
                           dtype={"pl_Id": sqlalchemy.types.VARCHAR(length=225),
                                  "channel_name": sqlalchemy.types.VARCHAR(length=225),
                                  "channel_id": sqlalchemy.types.VARCHAR(length=225),
                                  "publish_at": sqlalchemy.types.String(length=50),
                                  "videos_count": sqlalchemy.types.INT})

            channel_comment.to_sql(con=engine, name='comments', if_exists='append', index=False,
                                   dtype={'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                                          'Comment_Id': sqlalchemy.types.VARCHAR(length=225),
                                          'Comment_Text': sqlalchemy.types.TEXT,
                                          'Comment_Author': sqlalchemy.types.VARCHAR(length=225),
                                          'Comment_Published': sqlalchemy.types.String(length=50), })

            v_stats_df.to_sql(con=engine, name='video', if_exists='append', index=False,
                              dtype={'Channel_name': sqlalchemy.types.VARCHAR(length=225),
                                     'Channel_Id': sqlalchemy.types.VARCHAR(length=225),
                                     'Video_id': sqlalchemy.types.VARCHAR(length=225),
                                     'Title': sqlalchemy.types.VARCHAR(length=225),
                                     'Tags': sqlalchemy.types.VARCHAR(length=2000),
                                     'Thumbnail': sqlalchemy.types.VARCHAR(length=225),
                                     'Description': sqlalchemy.types.VARCHAR(length=8000),
                                     'Published_date': sqlalchemy.types.String(length=50),
                                     'Duration': sqlalchemy.types.VARCHAR(length=1024),
                                     'Views': sqlalchemy.types.BigInteger,
                                     'Likes': sqlalchemy.types.BigInteger,
                                     'Comments': sqlalchemy.types.INT,
                                     'Favorite_count': sqlalchemy.types.INT,
                                     'Caption_status': sqlalchemy.types.VARCHAR(length=225)})
            Migrate = st.button('**Success**')

# Analysis Zone
elif selected == "Analysis Zone":
    st.header('[Channel Data Analysis zone]')
    st.write('''(Checks for available channels by clicking this checkbox)''')
    # Check available channel data
    Check_channel = st.checkbox('**Check available channel data for analysis**')
    if Check_channel:
        # Create database connection
        engine = create_engine("mysql+pymysql://root:051092@localhost:3306/youtubedata")
        # Execute SQL query to retrieve channel names
        query = "SELECT channel_name FROM channels;"
        results = pd.read_sql(query, engine)
        # Get channel names as a list
        channel_names_fromsql = list(results['channel_name'])
        # Create a DataFrame from the list and reset the index to start from 1
        df_at_sql = pd.DataFrame(channel_names_fromsql, columns=['Available channel data']).reset_index(drop=True)
        # Reset index to start from 1 instead of 0
        df_at_sql.drop_duplicates(inplace=True)
        df_at_sql.index += 1
        # Show dataframe
        st.dataframe(df_at_sql)

# Query Zone
elif selected == "Query Zone":
    st.subheader('[Queries and Results ]')
    st.write('''(Queries were answered based on **Channel Data analysis**)''')

    # Selectbox creation
    question_tosql = st.selectbox('Select your Question]',
                                  ('1. What are the names of all the videos and their corresponding channels?',
                                   '2. Which channels have the most number of videos, and how many videos do they have?',
                                   '3. What are the top 10 most viewed videos and their respective channels?',
                                   '4. How many comments were made on each video, and what are their corresponding video names?',
                                   '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                   '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                   '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                   '8. What are the names of all the channels that have published videos in the year 2022?',
                                   '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                   '10. Which videos have the highest number of comments, and what are their corresponding channel names?'),
                                  key='collection_question')

    # Create a connection to SQL
    connect_for_question = pymysql.connect(host='localhost', user='root', password='051092', port=3306, db='youtubedata')
    cursor = connect_for_question.cursor()

    # Check selected option
    if question_tosql == '1. What are the names of all the videos and their corresponding channels?':
        sql_query = 'SELECT Title, Channel_name FROM video'
        results = pd.read_sql(sql_query, connect_for_question)
        st.write(results)

    elif question_tosql == '2. Which channels have the most number of videos, and how many videos do they have?':
        sql_query = 'SELECT Channel_name, COUNT(*) AS Video_Count FROM video GROUP BY Channel_name ORDER BY COUNT(*) DESC'
        results = pd.read_sql(sql_query, connect_for_question)
        st.write(results)

    elif question_tosql == '3. What are the top 10 most viewed videos and their respective channels?':
        sql_query = 'SELECT Title, Channel_name, Views FROM video ORDER BY Views DESC LIMIT 10'
        results = pd.read_sql(sql_query, connect_for_question)
        st.write(results)

    elif question_tosql == '4. How many comments were made on each video, and what are their corresponding video names?':
        sql_query = 'SELECT Title, COUNT(*) AS Comment_Count FROM comments INNER JOIN video ON comments.Video_Id = video.Video_id GROUP BY video.Title'
        results = pd.read_sql(sql_query, connect_for_question)
        st.write(results)

    elif question_tosql == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        sql_query = 'SELECT Title, Likes, Channel_name FROM video ORDER BY Likes DESC LIMIT 10'
        results = pd.read_sql(sql_query, connect_for_question)
        st.write(results)

    elif question_tosql == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        sql_query = 'SELECT Title, SUM(Likes) AS Total_Likes, SUM(Comments) AS Total_Comments FROM video GROUP BY Title'
        results = pd.read_sql(sql_query, connect_for_question)
        st.write(results)

    elif question_tosql == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        sql_query = 'SELECT Channel_name, SUM(Views) AS Total_Views FROM video GROUP BY Channel_name'
        results = pd.read_sql(sql_query, connect_for_question)
        st.write(results)

    elif question_tosql == '8. What are the names of all the channels that have published videos in the year 2022?':
        sql_query = "SELECT DISTINCT Channel_name FROM video WHERE YEAR(Published_date) = 2022"
        results = pd.read_sql(sql_query, connect_for_question)
        st.write(results)

    elif question_tosql == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        sql_query = 'SELECT Channel_name, AVG(DURATION) AS Avg_Duration FROM video GROUP BY Channel_name'
        results = pd.read_sql(sql_query, connect_for_question)
        st.write(results)

    elif question_tosql == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        sql_query = 'SELECT Title, Comments, Channel_name FROM video ORDER BY Comments DESC LIMIT 10'
        results = pd.read_sql(sql_query, connect_for_question)
        st.write(results)

    # Close connection
    connect_for_question.close()