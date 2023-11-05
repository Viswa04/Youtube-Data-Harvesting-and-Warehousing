from googleapiclient.discovery import build
import pymongo
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import urllib
import ssl
import psycopg2
import pandas as pd
import streamlit as st

# API key connection

def Api_connect():
    Api_id = "AIzaSyD4YTIKfmqGhKujvC5TNHlWGXfvLo4Bxr8"
    Api_service_name = "youtube"
    Api_version = "v3"

    youtube = build(Api_service_name,Api_version, developerKey = Api_id)

    return youtube
youtube = Api_connect()

# Get Channel information
def get_channel_info(channel_id):
    request = youtube.channels().list(
        part = "snippet,contentDetails,statistics",
        id = channel_id)
    response = request.execute()

    for i in response['items']:
        data = dict(Channel_Name = i['snippet']['title'],
                    Channel_Id = i['id'],
                    Subscribers = i['statistics']['subscriberCount'],
                    Views = i['statistics']['viewCount'],
                    Total_Videos = i['statistics']['videoCount'],
                    Channel_Description = i['snippet']['description'],
                    Playlist_Id = i['contentDetails']['relatedPlaylists']['uploads'])
    return data

# get videos ids
def get_videos_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(
        id = channel_id,
        part = 'contentDetails').execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            part = 'snippet',
            playlistId = playlist_id,
            maxResults = 50,
            pageToken = next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

# Get Video Information
def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part = 'snippet,contentDetails,statistics',
            id = video_id
        )
        response = request.execute()

        for item in response['items']:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet']['description'],
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption'],
                        )
            video_data.append(data)
    return video_data

# Get Comment Information
def get_comment_info(video_ids):
    Comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part = 'snippet',
                videoId = video_id,
                maxResults = 50
            )
            response = request.execute()

            for item in response['items']:
                data = dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                            Video_Id = item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published = item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)

    except:
        pass
    return Comment_data

# Get playlist details
def get_playlist_details(channel_id):
    next_page_token = None
    All_data = []

    while True:
        request = youtube.playlists().list(
            part = 'snippet,contentDetails',
            channelId = channel_id,
            maxResults = 50,
            pageToken = next_page_token
        )
        response = request.execute()

        for item in response['items']:
            data = dict(Playlist_Id = item['id'],
                        Title = item['snippet']['title'],
                        Channel_Id = item['snippet']['channelId'],
                        Channel_Name = item['snippet']['channelTitle'],
                        PublishedAt = item['snippet']['publishedAt'],
                        Video_Count = item['contentDetails']['itemCount'])
            
            All_data.append(data)
        
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data

# MongoDB Connect

username = "kviswa004"
password = "Viswa@04"
# password
# Encode the username and password

encoded_username = urllib.parse.quote_plus(username)
encoded_password = urllib.parse.quote_plus(password)


# Construct the URI with encoded credentials


uri = f"mongodb+srv://{encoded_username}:{encoded_password}@cluster0.omqgkpo.mongodb.net/?retryWrites=true&w=majority&appName=AtlasApp"


# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'), tz_aware=False, connect=True)

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(f"Connection failed: {e}")

# Uploading data to MongoDB

db = client["Youtube_data"]

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_ids = get_videos_ids(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)

    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,
                      "playlist_information":pl_details,
                      "video_information": vi_details,
                      "comment_information": com_details})
    
    return "Upload completed successfully"

# Table creation for channels in SQL

def channels_table():
    mydb = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "Viswa@04",
                            database = "Youtube_data",
                            port = "5432")

    cursor = mydb.cursor()

    try:
        create_query = '''Create Table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscribers bigint,
                                                            Views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        st.write("Channels table already created")

    # Channel information from MongoDB
    ch_list = []
    db = client["Youtube_data"]
    coll1 = db['channel_details']
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])

    ch_df = pd.DataFrame(ch_list)

    for index,row in ch_df.iterrows():
        insert_query = '''insert into channels(Channel_Name,
                                                Channel_Id,
                                                Subscribers,
                                                Views,
                                                Total_Videos,
                                                Channel_Description,
                                                Playlist_Id)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        
        except:
            st.write("Channel values are already inserted")

# Table creation for playlist in SQL

def playlists_table():
    mydb = psycopg2.connect(host = "localhost",
                                user = "postgres",
                                password = "Viswa@04",
                                database = "Youtube_data",
                                port = "5432")

    cursor = mydb.cursor()

    drop_query = "drop table if exists playlists"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''Create Table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                                Title varchar(80),
                                                                Channel_Id varchar(100),
                                                                Channel_Name varchar(100),
                                                                PublishedAt timestamp,
                                                                Video_Count int)'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        st.write("Playlists table already created")

    # playlist information from MongoDB
    pl_list = []
    db = client["Youtube_data"]
    coll1 = db['channel_details']
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])

    pl_df = pd.DataFrame(pl_list)

    for index,row in pl_df.iterrows():
        insert_query = '''insert into playlists(Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                PublishedAt,
                                                Video_Count)
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''
        
        values = (row['Playlist_Id'],
                    row['Title'],
                    row['Channel_Id'],
                    row['Channel_Name'],
                    row['PublishedAt'],
                    row['Video_Count'])
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
            
        except:
            st.write("Playlists values are already inserted")

# Table creation for videos in SQL

def videos_table():
    mydb = psycopg2.connect(host = "localhost",
                                    user = "postgres",
                                    password = "Viswa@04",
                                    database = "Youtube_data",
                                    port = "5432")

    cursor = mydb.cursor()

    drop_query = "drop table if exists videos"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''Create Table if not exists videos(Channel_Name varchar(150),
                                                            Channel_Id varchar(100),
                                                            Video_Id varchar(50) primary key,
                                                            Title varchar(150),
                                                            Tags text,
                                                            Thumbnail varchar(250),
                                                            Description text,
                                                            Published_Date timestamp,
                                                            Duration interval,
                                                            Views bigint,
                                                            Likes bigint,
                                                            Comments int,
                                                            Favorite_Count int,
                                                            Definition varchar(10),
                                                            Caption_Status varchar(50))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        st.write("Videos table already created")

    # video information from MongoDB
    vi_list = []
    db = client["Youtube_data"]
    coll1 = db['channel_details']
    for vl_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vl_data["video_information"])):
            vi_list.append(vl_data["video_information"][i])

    vi_df = pd.DataFrame(vi_list)

    for index,row in vi_df.iterrows():
        insert_query = '''insert into videos(Channel_Name,
                                            Channel_Id,
                                            Video_Id,
                                            Title,
                                            Tags,
                                            Thumbnail,
                                            Description,
                                            Published_Date,
                                            Duration,
                                            Views,
                                            Likes,
                                            Comments,
                                            Favorite_Count,
                                            Definition,
                                            Caption_Status)
                                                
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        
        values = (row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Tags'],
                row['Thumbnail'],
                row['Description'],
                row['Published_Date'],
                row['Duration'],
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Favorite_Count'],
                row['Definition'],
                row['Caption_Status'])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
                
        except:
            st.write("Videos values are already inserted")

# Table creation for comments in SQL

def comments_table():
    mydb = psycopg2.connect(host = "localhost",
                                    user = "postgres",
                                    password = "Viswa@04",
                                    database = "Youtube_data",
                                    port = "5432")

    cursor = mydb.cursor()

    drop_query = "drop table if exists comments"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''Create Table if not exists comments(Comment_Id varchar(150) primary key,
                                                            Video_Id varchar(100),
                                                            Comment_Text text,
                                                            Comment_Author varchar(100),
                                                            Comment_Published timestamp)'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        st.write("Comments table already created")

    # comments information from MongoDB
    comm_list = []
    db = client["Youtube_data"]
    coll1 = db['channel_details']
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            comm_list.append(com_data["comment_information"][i])

    com_df = pd.DataFrame(comm_list)

    for index,row in com_df.iterrows():
        insert_query = '''insert into comments(Comment_Id,
                                            Video_Id,
                                            Comment_Text,
                                            Comment_Author,
                                            Comment_Published)
                                                
                                            values(%s,%s,%s,%s,%s)'''
        
        values = (row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published'])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
                
        except:
            st.write("Comments values are already inserted")

def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    return "Tables created successfully"

def show_channels_table():
    ch_list = []
    db = client["Youtube_data"]
    coll1 = db['channel_details']
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])

    channels_table = st.dataframe(ch_list)
    return channels_table

def show_playlists_table():
    pl_list = []
    db = client["Youtube_data"]
    coll1 = db['channel_details']
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])

    playlists_table = st.dataframe(pl_list)
    return playlists_table

def show_videos_table():
    vi_list = []
    db = client["Youtube_data"]
    coll1 = db['channel_details']
    for vl_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vl_data["video_information"])):
            vi_list.append(vl_data["video_information"][i])

    videos_table = st.dataframe(vi_list)
    return videos_table

def show_comments_table():
    comm_list = []
    db = client["Youtube_data"]
    coll1 = db['channel_details']
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            comm_list.append(com_data["comment_information"][i])

    comments_table = st.dataframe(comm_list)
    return comments_table

Channel_Name = ['Simply_Sarath','Gray_Wolf','Avant_Grande','RishiPedia','Being_Jagan',
                'Madras_Central','Akash_J','Mr._GK','Saravanan_Decodes','Science_with_Sam']

Channel_Ids = ['UC2TvVGvdIBKB5FKLzhIsN6A','UCrVCVrVsJ558rfsgkj8bfFA','UCey3Dwkxv_ztdU2jyMhOCww','UCaFBtetU2RHxwnDRDsNHejA','UCkW1GqQN_cDhl_7MrajLeoQ',
'UCGBnz-FR3qaowYsyIEh2-zw','UCZ_D5I9s3cf8T9306Uzj5Ag','UC5cY198GU1MQMIPJgMkCJ_Q','UCk081mmVz4hzff-3YVBAxow','UChGd9JY4yMegY6PxqpBjpRA']

Sample_channel_Ids = {"Channel_Name" : Channel_Name, "Channel_Ids" : Channel_Ids}
Sample_channel_Ids = pd.DataFrame(Sample_channel_Ids)
Sample_channel_Ids.index = Sample_channel_Ids.index + 1

st.title(":red[Youtube Data Harvesting and Warehousing]")

col1,col2,col3 = st.columns(3)

with col1:
    if st.button(":red[Skills Take Away]"):
        st.write("* Python Scripting")
        st.write("* Data Collection")
        st.write("* MongoDB")
        st.write("* Streamlit")
        st.write("* API Integration")
        st.write("* Data Management using MongoDB and SQL")

with col2:
    if st.button(":red[Sample Channel IDs]"):
        st.table(Sample_channel_Ids)


st.subheader(":red[Upload to MongoDB]")
data = st.text_input("Enter the Channel ID")
if st.button(":red[Store Data to MongoDB]"):
    store = channel_details(data)
    st.success(store)

st.subheader(":red[Migrate Data to SQL]")
if st.button(":red[Migrate to SQL]"):
    data = tables()
    st.success(data)

st.markdown("#### :red[View Table]")
view_table = st.selectbox("Select an option",["None","channels","playlists", "videos", "comments"])

if view_table == "channels":
    show_channels_table()
if view_table == "playlists":
    show_playlists_table()
if view_table == "videos":
    show_videos_table()
if view_table == "comments":
    show_comments_table()

# SQL connection

mydb = psycopg2.connect(host = "localhost",
                                user = "postgres",
                                password = "Viswa@04",
                                database = "Youtube_data",
                                port = "5432")

cursor = mydb.cursor()

st.subheader(":red[Data Analysis]")
Questions = st.selectbox("Select a question",["None","1. What are all the names of all the videos and their corresponding channels?",
                                              "2. Which channels have the most number of videos, and how many videos do they have?",
                                              "3. What are the top 10 most viewed videos and their respective channels?",
                                              "4. How many comments were made on each video, and what are their corresponding video names?",
                                              "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                              "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                              "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                              "8. What are the names of all the channels that have published videos in the year 2022?",
                                              "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                              "10. Which videos have the highest number of comments, and what are their corresponding channel names"])

if Questions == "1. What are all the names of all the videos and their corresponding channels?":
    ques1 = "select title as Videos, channel_name from videos"
    cursor.execute(ques1)
    mydb.commit()
    data = cursor.fetchall()
    table1 = pd.DataFrame(data, columns = ["Video_Title","Channel_Name"])
    table1.index += 1
    st.table(table1)

elif Questions == "2. Which channels have the most number of videos, and how many videos do they have?":
    ques2 = "select channel_name,total_videos from channels order by total_videos desc"
    cursor.execute(ques2)
    mydb.commit()
    data = cursor.fetchall()
    table2 = pd.DataFrame(data, columns = ["Channel_Name","Number_Of_Videos"])
    table2.index += 1
    st.table(table2)

elif Questions == "3. What are the top 10 most viewed videos and their respective channels?":
    ques3 = "select channel_name,title,views from videos where views is not null order by views desc limit 10"
    cursor.execute(ques3)
    mydb.commit()
    data = cursor.fetchall()
    table3 = pd.DataFrame(data, columns = ["Channel_Name","Video_Title","Total_Views"])
    table3.index += 1
    st.table(table3)

elif Questions == "4. How many comments were made on each video, and what are their corresponding video names?":
    ques4 = "select title,comments from videos where comments is not null order by comments desc"
    cursor.execute(ques4)
    mydb.commit()
    data = cursor.fetchall()
    table4 = pd.DataFrame(data, columns = ["Video_Title","Number_Of_Comments"])
    table4.index += 1
    st.table(table4)

elif Questions == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    ques5 = "select channel_name, title,likes from videos where likes is not null order by likes desc limit 20"
    cursor.execute(ques5)
    mydb.commit()
    data = cursor.fetchall()
    table5 = pd.DataFrame(data, columns = ["Channel_Name","Video_Title","Number_Of_Likes"])
    table5.index += 1
    st.table(table5)

elif Questions == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    ques6 = "select title,likes from videos where likes is not null order by likes desc"
    cursor.execute(ques6)
    mydb.commit()
    data = cursor.fetchall()
    table6 = pd.DataFrame(data, columns = ["Video_Title","Number_Of_Likes"])
    table6.index += 1
    st.write("### :red[Youtube had removed Dislike button]")
    st.table(table6)

elif Questions == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
    ques7 = "select channel_name, views from channels order by views desc"
    cursor.execute(ques7)
    mydb.commit()
    data = cursor.fetchall()
    table7 = pd.DataFrame(data, columns = ["Channel_Name","Number_Of_Views"])
    table7.index += 1
    st.table(table7)

elif Questions == "8. What are the names of all the channels that have published videos in the year 2022?":
    ques8 = "select channel_name,title,published_date from videos where extract(year from published_date)=2022 order by published_date desc"
    cursor.execute(ques8)
    mydb.commit()
    data = cursor.fetchall()
    table8 = pd.DataFrame(data, columns = ["Channel_Name","Video_Title","Published_Date"])
    table8.index += 1
    st.table(table8)

elif Questions == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    ques9 = "select channel_name,avg(duration) as Average_Duration from videos group by channel_name"
    cursor.execute(ques9)
    mydb.commit()
    table9=cursor.fetchall()
    table9 = pd.DataFrame(table9, columns=['Channel_Name', 'Average_Duration'])
    T9=[]
    for index, row in table9.iterrows():
        channel_title = row['Channel_Name']
        average_duration = row['Average_Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    T9 = pd.DataFrame(T9)
    T9.index += 1
    st.table(T9)

elif Questions == "10. Which videos have the highest number of comments, and what are their corresponding channel names":
    ques10 = "select channel_name,title,comments from videos where comments is not null order by comments desc limit 20"
    cursor.execute(ques10)
    mydb.commit()
    data = cursor.fetchall()
    table10 = pd.DataFrame(data, columns = ["Channel_Name","Video_Title","Number_Of_Comments"])
    table10.index += 1
    st.table(table10)



