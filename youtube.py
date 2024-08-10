import streamlit as st
from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd

# API key connection
def Api_connect():
    Api_ID = "AIzaSyDq1uDZkVH1xMVQtI2YHJ72hVLyRNHWBWI"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=Api_ID)
    return youtube

youtube = Api_connect()

# Channel info
def get_channel_info(channel_id):
    request = youtube.channels().list(part="snippet,ContentDetails,statistics", id=channel_id)
    response = request.execute()
    for i in response['items']:
        data = dict(
            channel_Name=i["snippet"]["title"],
            channel_Id=i["id"],
            Subscribers=i['statistics']['subscriberCount'],
            Views=i["statistics"]['viewCount'],
            Total_Videos=i["statistics"]['videoCount'],
            channel_Description=i["snippet"]["description"],
            playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"]
        )
    return data

# Get video IDs
def get_videos_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True:
        response1 = youtube.playlistItems().list(part='snippet', playlistId=Playlist_Id, maxResults=50, pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids                                      

# Get video information
def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(part="snippet,ContentDetails,statistics", id=video_id)
        response = request.execute()
        for item in response["items"]:
            data = dict(
                channels_Name=item['snippet']['channelTitle'],
                channel_Id=item['snippet']['channelId'],
                video_Id=item['id'],
                Title=item['snippet']['title'],
                Tags=item['snippet'].get('tags'),
                Thumbnail=item['snippet']['thumbnails']['default']['url'],
                Description=item['snippet'].get('description'),
                Published_Date=item['snippet']['publishedAt'],
                Duration=item['contentDetails']['duration'],
                Views=item['statistics'].get('viewCount'),
                Likes=item['statistics'].get('likeCount'),
                comments=item['statistics'].get('commentCount'),
                Favorite_count=item['statistics']['favoriteCount'],
                Definition=item['contentDetails']['definition'],
                Caption_Status=item['contentDetails']['caption']
            )
            video_data.append(data)
    return video_data

# Get comment info
def get_comment_info(video_ids):
    comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(part="snippet", videoId=video_id, maxResults=50)
            response = request.execute()
            for item in response['items']:
                data = dict(
                    Comment_Id=item['snippet']['topLevelComment']['id'],
                    Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                    Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                comment_data.append(data)
    except:
        pass
    return comment_data            

# Get playlist details
def get_playlist_details(channel_id):
    next_page_token = None
    All_data = []
    while True:
        request = youtube.playlists().list(part='snippet,contentDetails', channelId=channel_id, maxResults=50, pageToken=next_page_token)
        response = request.execute()
        for item in response['items']:
            data = dict(
                Playlist_Id=item['id'],
                Title=item['snippet']['title'],
                Channel_Id=item['snippet']['channelId'],
                Channel_Name=item['snippet']['channelTitle'],
                publishedAt=item['snippet']['publishedAt'],
                Video_Count=item['contentDetails']['itemCount']
            )
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data

# Upload to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["youtube_data_harvesting"]

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_ids = get_videos_ids(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)
    coll1 = db["channel_details"]
    coll1.insert_one({
        "channel_information": ch_details,
        "playlist_information": pl_details,
        "video_information": vi_details,
        "comment_information": com_details
    })
    return "Upload completed successfully"

# Table creation for channels, playlists, videos, comments
def channels_table(name_of_channel):
    mydb = psycopg2.connect(host="localhost", user="postgres", password="root", database="youtube_data", port="5432")
    cursor = mydb.cursor()
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS channels (
            channel_Name VARCHAR(100),
            channel_Id VARCHAR(80) PRIMARY KEY,
            Subscribers BIGINT,
            Views BIGINT,
            Total_Videos INT,
            channel_Description TEXT,
            playlist_Id VARCHAR(80)
        )'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("Channels table already created")
    single_channel_detail = []
    coll1 = db["channel_details"]
    for ch_data in coll1.find({"channel_information.channel_Name": name_of_channel}, {"_id": 0}):
        single_channel_detail.append(ch_data["channel_information"])
    df_single_channel_detail = pd.DataFrame(single_channel_detail)
    for index, row in df_single_channel_detail.iterrows():
        insert_query = '''INSERT INTO channels (
            channel_Name, channel_Id, Subscribers, Views, Total_Videos, channel_Description, playlist_Id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)'''
        values = (
            row['channel_Name'], row['channel_Id'], row['Subscribers'], row['Views'],
            row['Total_Videos'], row['channel_Description'], row['playlist_Id']
        )
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            news = f"Your provided channel name {name_of_channel} already exists"
            return news

def playlist_table(name_of_channel):
    mydb = psycopg2.connect(host="localhost", user="postgres", password="root", database="youtube_data", port="5432")
    cursor = mydb.cursor()
    create_query = '''CREATE TABLE IF NOT EXISTS playlists (
        Playlist_Id VARCHAR(100) PRIMARY KEY,
        Title VARCHAR(100),
        Channel_Id VARCHAR(100),
        Channel_Name VARCHAR(100),
        publishedAt TIMESTAMP,
        Video_Count INT
    )'''
    cursor.execute(create_query)
    mydb.commit()
    single_playlist_details = []
    coll1 = db["channel_details"]
    for ch_data in coll1.find({"channel_information.channel_Name": name_of_channel}, {"_id": 0}):
        single_playlist_details.append(ch_data["playlist_information"])
    df_single_playlist_details = pd.DataFrame(single_playlist_details[0])
    for index, row in df_single_playlist_details.iterrows():
        insert_query = '''INSERT INTO playlists (
            Playlist_Id, Title, Channel_Id, Channel_Name, publishedAt, Video_Count
        ) VALUES (%s, %s, %s, %s, %s, %s)'''
        values = (
            row['Playlist_Id'], row['Title'], row['Channel_Id'], row['Channel_Name'],
            row['publishedAt'], row['Video_Count']
        )
        cursor.execute(insert_query, values)
        mydb.commit()

def videos_table(name_of_channel):
    mydb = psycopg2.connect(host="localhost", user="postgres", password="root", database="youtube_data", port="5432")
    cursor = mydb.cursor()
    create_query = '''CREATE TABLE IF NOT EXISTS videos (
        channels_Name VARCHAR(100),
        channel_Id VARCHAR(100),
        video_Id VARCHAR(30) PRIMARY KEY,
        Title VARCHAR(150),
        Tags TEXT,
        Thumbnail VARCHAR(200),
        Description TEXT,
        Published_Date TIMESTAMP,
        Duration INTERVAL,
        Views BIGINT,
        Likes BIGINT,
        comments INT,
        Favourite_count INT,
        Definition VARCHAR(10),
        Caption_Status VARCHAR(50)
    )'''
    cursor.execute(create_query)
    single_video_details = []
    coll1 = db["channel_details"]
    for ch_data in coll1.find({"channel_information.channel_Name": name_of_channel}, {"_id": 0}):
        single_video_details.append(ch_data["video_information"])
    df_single_video_details = pd.DataFrame(single_video_details[0])
    for index, row in df_single_video_details.iterrows():
        insert_query = '''INSERT INTO videos (
            channels_Name, channel_Id, video_Id, Title, Tags, Thumbnail, Description,
            Published_Date, Duration, Views, Likes, comments, Favourite_count, Definition, Caption_Status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        values = (
            row['channels_Name'], row['channel_Id'], row['video_Id'], row['Title'], row['Tags'],
            row['Thumbnail'], row['Description'], row['Published_Date'], row['Duration'], row['Views'],
            row['Likes'], row['comments'], row['Favourite_count'], row['Definition'], row['Caption_Status']
        )
        cursor.execute(insert_query, values)
        mydb.commit()

def comments_table(name_of_channel):
    mydb = psycopg2.connect(host="localhost", user="postgres", password="root", database="youtube_data", port="5432")
    cursor = mydb.cursor()
    create_query = '''CREATE TABLE IF NOT EXISTS comments (
        Comment_Id TEXT PRIMARY KEY,
        Video_Id TEXT,
        Comment_Text TEXT,
        Comment_Author TEXT,
        Comment_Published TIMESTAMP
    )'''
    cursor.execute(create_query)
    single_comment_details = []
    coll1 = db["channel_details"]
    for ch_data in coll1.find({"channel_information.channel_Name": name_of_channel}, {"_id": 0}):
        single_comment_details.append(ch_data["comment_information"])
    df_single_comment_details = pd.DataFrame(single_comment_details[0])
    for index, row in df_single_comment_details.iterrows():
        insert_query = '''INSERT INTO comments (
            Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Published
        ) VALUES (%s, %s, %s, %s, %s)'''
        values = (
            row['Comment_Id'], row['Video_Id'], row['Comment_Text'], row['Comment_Author'], row['Comment_Published']
        )
        cursor.execute(insert_query, values)
        mydb.commit()

# Streamlit interface
st.title("YouTube Data Harvesting")

# Channel ID input
channel_id = st.text_input("Enter the YouTube Channel ID")

if st.button("Fetch and Store Channel Data"):
    if channel_id:
        result = channel_details(channel_id)
        st.success(result)
    else:
        st.error("Please enter a valid Channel ID")

# Display channel info
if channel_id:
    st.subheader("Channel Information")
    channel_info = get_channel_info(channel_id)
    st.write(channel_info)

mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="root",
                    database="youtube_data",
                    port="5432")
cursor=mydb.cursor()

# Questions
question=st.selectbox("select your question",("1. All the videos and the channel name",
                                              "2. channels with most number of videos",
                                              "3. 10 most viewed videos",
                                              "4. comments in each videos",
                                              "5. videos with highest likes",
                                              "6. likes of all videos",
                                              "7. views of each channel",
                                              "8. videos published in the year of 2022",
                                              "9. average duration of all videos in each channel",
                                              "10. videos with highest number of comments"))
if question=="1. All the videos and the channel name":
    query1='''select title as videos,channels_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif question=="2. channels with most number of videos":
    query2='''select channel_name as channelname,total_videos as no_videos from channels
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","no of videos"])
    st.write(df2)

elif question=="3. 10 most viewed videos":
    query3='''select views as views,channels_name as channelname,title as videotitle from videos
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)

elif question=="4. comments in each videos":
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)

elif question== "5. videos with highest likes":
    query5='''select title videotitle,channels_name as channelname,likes as likecount
                from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question== "6. likes of all videos":
    query6='''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)

elif question=="7. views of each channel":
    query7='''select channel_name as channelname, views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
    st.write(df7)

elif question=="8. videos published in the year of 2022":
    query8='''select title as video_title,published_date as videorelease,channels_name as channelname from videos where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
    st.write(df8)


elif question=="9. average duration of all videos in each channel":
    query9='''select channels_name as channelname,AVG(duration) as averageduration from videos group by channels_name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])
    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)

elif question=="10. videos with highest number of comments":
    query10='''select title as videotitle,channels_name as channelname,comments as comments from videos where comments is 
                not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["videotitle","channelname","comments"])
    st.write(df10)
