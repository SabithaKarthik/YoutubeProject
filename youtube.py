from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import sqlalchemy as sqldb
import streamlit as st

#Establish API connection
def API_connect():
    api_Key="AIzaSyCwFwwbnCJfk7gVeLTEPa81W0Cdup6a7ss"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=api_Key)
    return youtube

youtube=API_connect()

#UCGq-a57w-aPwyi3pW7XLiHw - The Diary Of A CEO
#UCuI5XcJYynHa5k_lqDzAgwQ - data Science tamil
#UCaRMivfyupj3ucUyJbZbCNg - Anglo-link
#UCMHBhFenH7VFL4gcJtlNfEg - Divya sreeji channel
#UCMAMi60f1cG4oTCZC1M2ajg - Suvaithiru

#get Channel details
def getChannelInfo(channel_id):
    request=youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    for i in response['items']:
        channelData=dict(channel_id=i['id'], channel_name=i['snippet']['title'],
                channel_description=i['snippet']['description'],
                channel_views=i['statistics']['viewCount'],
                subscriber_count=i['statistics']['subscriberCount'],
                video_count=i['statistics']['videoCount'],
                playlist_id=i['contentDetails']['relatedPlaylists']['uploads'])
    return channelData

#get video Ids
def getVideoIds(channel_id):
    request=youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    video_ids=[]
    nextPageToken=None
    while True:
        request1=youtube.playlistItems().list(
            part="snippet",
            playlistId=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            maxResults=50,
            pageToken=nextPageToken
        )
        response1=request1.execute()   
        for i in range(len(response1['items'])):
            videoId=response1['items'][i]['snippet']['resourceId']['videoId']
            video_ids.append(videoId)
        nextPageToken=response1.get("nextPageToken")       
        if nextPageToken is None:
            break
    return video_ids

#get video details
def getVideoDetails(video_ids):
    video_list=[]
    for video_id in video_ids:
        request=youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
            )
        response = request.execute()
        for i in response['items']:
            videoData=dict(channel_id=i['snippet']['channelId'],
                        video_id=i['id'],
                        video_name=i['snippet']['title'],
                        thumbnail=i['snippet']['thumbnails']['default']['url'],
                        video_description=i['snippet']['description'],
                        published_date=i['snippet']['publishedAt'],
                        duration=i['contentDetails']['duration'],
                        view_count=i['statistics']['viewCount'],
                        like_count=i['statistics'].get('likeCount'),
                        comment_count=i['statistics'].get('commentCount'),
                        favorite_count=i['statistics']['favoriteCount'],
                        caption_status=i['contentDetails']['caption'])
            video_list.append(videoData)
    return video_list

#get comment details
def getCommentDetails(video_ids):
    comment_list=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                        part="snippet",
                        videoId=video_id,
                        maxResults=50
                    )
            response = request.execute()
            for i in response['items']:
                commentData=dict(comment_id=i['snippet']['topLevelComment']['id'],
                                video_id=i['snippet']['topLevelComment']['snippet']['videoId'],
                                comment_text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                                comment_author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                comment_published_date=i['snippet']['topLevelComment']['snippet']['publishedAt'])
                comment_list.append(commentData)
            return comment_list
    except:
        pass

#get playlist details
def getPlaylistDetails(channel_id):
    playlist=[]
    nextPageToken=None
    while True:
        request=youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=50,
                pageToken=nextPageToken
            )
        response = request.execute()
        for i in response['items']:
            playlistData=dict(playlist_id=i['id'],
                            playlist_name=i['snippet']['title'],
                            channel_id=i['snippet']['channelId'])
            playlist.append(playlistData)
        nextPageToken=response.get("nextPageToken")       
        if nextPageToken is None:
            break
    return playlist

#Establish connection to MongoDB
client=pymongo.MongoClient("mongodb+srv://sabitha:Ganesh12@cluster0.vtvkffr.mongodb.net/?retryWrites=true&w=majority")
db=client["Youtube_data"]

#Insert into MongoDB
def insertIntoMongoDB(channel_id):
    try:
        channelDetails=getChannelInfo(channel_id)
        video_ids=getVideoIds(channel_id)
        playlistDetails=getPlaylistDetails(channel_id)
        videoDetails=getVideoDetails(video_ids)
        commentDetails=getCommentDetails(video_ids)
        coll=db["channel_details"]
        coll.insert_one({'channel_information':channelDetails,'playlist_information':playlistDetails,'video_information':videoDetails,'comment-information':commentDetails})
        return "Channel :blue[[*" + channel_id + "*]] details has been successfully inserted into MongoDB"
    except Exception as e:
        st.write(e)
        

#Establish connection to SQL
engine = sqldb.create_engine("postgresql+psycopg2://postgres:12345@localhost/youtubeData")
connection = engine.connect()
metadata = sqldb.MetaData()
    
#create channel table and insert values
def insertChannelData(channel_id):
    channel_list=[]
    db=client["Youtube_data"]
    coll=db["channel_details"]
    for ch_data in coll.find({"channel_information.channel_id":channel_id},{"_id":0,"channel_information":1}):
        channel_list.append(ch_data["channel_information"])
    df=pd.DataFrame(channel_list)
    if not sqldb.inspect(engine).has_table("channel"):
        emp = sqldb.Table('channel', metadata,
                    sqldb.Column('channel_id', sqldb.String(80), primary_key=True),
                    sqldb.Column('channel_name', sqldb.String(80)),
                    sqldb.Column('channel_description', sqldb.String()),
                    sqldb.Column('channel_views', sqldb.Integer()),
                    sqldb.Column('subscriber_count', sqldb.Integer()),
                    sqldb.Column('video_count', sqldb.Integer()),
                    sqldb.Column('playlist_id', sqldb.String(80))
                    )                                                    
        metadata.create_all(engine)
    try:
        df.to_sql('channel', engine,if_exists='append',index=False)
    except Exception as e:
        st.write(e)

#Insert into playlist table
def insertPlaylistData(channel_id):
    playlist_list=[]
    db=client["Youtube_data"]
    coll=db["channel_details"]
    for pl_data in coll.find({"channel_information.channel_id":channel_id},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            playlist_list.append(pl_data["playlist_information"][i])
    df=pd.DataFrame(playlist_list)
    if not sqldb.inspect(engine).has_table("playlist"):
        emp = sqldb.Table('playlist', metadata,
                    sqldb.Column('playlist_id', sqldb.String(80), primary_key=True),
                    sqldb.Column('playlist_name', sqldb.String(80)),
                    sqldb.Column('channel_id', sqldb.String(80))
                    )                                                    
        metadata.create_all(engine)
    try:
        df.to_sql('playlist', engine,if_exists='append',index=False)
    except Exception as e:
        st.write(e)
        
#insert into video table
def insertVideoData(channel_id):
    video_list=[]
    db=client["Youtube_data"]
    coll=db["channel_details"]
    for vl_data in coll.find({"channel_information.channel_id":channel_id},{"_id":0,"video_information":1}):
        for i in range(len(vl_data["video_information"])):
            video_list.append(vl_data["video_information"][i])
    df=pd.DataFrame(video_list)
    if not sqldb.inspect(engine).has_table("video"):
        emp = sqldb.Table('video', metadata,
                    sqldb.Column('channel_id', sqldb.String(80)),
                    sqldb.Column('video_id', sqldb.String(80), primary_key=True),
                    sqldb.Column('video_name', sqldb.String(255)),
                    sqldb.Column('thumbnail', sqldb.String(80)),
                    sqldb.Column('video_description', sqldb.String()),
                    sqldb.Column('published_date', sqldb.DateTime),
                    sqldb.Column('duration', sqldb.Interval),
                    sqldb.Column('view_count', sqldb.Integer()),
                    sqldb.Column('like_count', sqldb.Integer()),
                    sqldb.Column('comment_count', sqldb.Integer()),
                    sqldb.Column('favorite_count', sqldb.Integer()),
                    sqldb.Column('caption_status', sqldb.String(80))
                    )                                                    
        metadata.create_all(engine)
    try:
        df.to_sql('video', engine,if_exists='append',index=False)
    except Exception as e:
        st.write(e)
    
#insert into comments table
def insertCommentData(channel_id):
    comment_list=[]
    db=client["Youtube_data"]
    coll=db["channel_details"]
    for cl_data in coll.find({"channel_information.channel_id":channel_id},{"_id":0,"comment-information":1}):
        for i in range(len(cl_data["comment-information"])):
            comment_list.append(cl_data["comment-information"][i])
    df=pd.DataFrame(comment_list)
    if not sqldb.inspect(engine).has_table("comment"):
        emp = sqldb.Table('comment', metadata,
                    sqldb.Column('comment_id', sqldb.String(80),primary_key=True),
                    sqldb.Column('video_id', sqldb.String(80)),
                    sqldb.Column('comment_text', sqldb.String()),
                    sqldb.Column('comment_author', sqldb.String()),
                    sqldb.Column('comment_published_date', sqldb.DateTime),
                    )                                                    
        metadata.create_all(engine)
    try:
        df.to_sql('comment', engine,if_exists='append',index=False)
    except Exception as e:
        st.write(e)

#Insert into SQL tables
def insertIntoSQL(channel_id):
    try:
        insertChannelData(channel_id)
        insertPlaylistData(channel_id)
        insertVideoData(channel_id)
        insertCommentData(channel_id)
        return "Successfully migrated the data into SQL tables"
    except Exception as e:
        st.write(e)

def show_channels_table():
    ch_list = []
    coll1 = db["channel_details"] 
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    channels_table = st.dataframe(ch_list)
    return channels_table

def show_playlists_table():
    coll1 =db["channel_details"]
    pl_list = []
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])
    playlists_table = st.dataframe(pl_list)
    return playlists_table

def show_videos_table():
    vi_list = []
    coll2 = db["channel_details"]
    for vi_data in coll2.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    videos_table = st.dataframe(vi_list)
    return videos_table

def show_comments_table():
    com_list = []
    coll3 = db["channel_details"]
    for com_data in coll3.find({},{"_id":0,"comment-information":1}):
        for i in range(len(com_data["comment-information"])):
            com_list.append(com_data["comment-information"][i])
    comments_table = st.dataframe(com_list)
    return comments_table

st.header(":red[YouTube Data Harvesting and Warehousing]")
st.divider()
channel_ids = st.text_input("Enter the Channel id")
channels = channel_ids.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Collect and Store data"):
    for channel in channels:
        ch_ids = []
        coll1 = db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["channel_id"])
        if channel in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exists")
        else:
            output = insertIntoMongoDB(channel)
            st.success(output)
            display = insertIntoSQL(channel)
            st.success(display)
            
show_table = st.radio(":red[SELECT THE TABLE FOR VIEW]",("Channels","Playlists","Videos","Comments"))

if show_table == "Channels":
    show_channels_table()
elif show_table == "Playlists":
    show_playlists_table()
elif show_table =="Videos":
    show_videos_table()
elif show_table == "Comments":
    show_comments_table()

if sqldb.inspect(engine).has_table("channel") and sqldb.inspect(engine).has_table("video"):
    metadata.reflect(bind=engine)
    channel = metadata.tables['channel']
    video = metadata.tables['video']
    question = st.selectbox(
        'Please Select Your Question',
        ('1. All the videos and the Channel Name',
        '2. Channels with most number of videos',
        '3. 10 most viewed videos',
        '4. Comments in each video',
        '5. Videos with highest likes',
        '6. likes of all videos',
        '7. views of each channel',
        '8. videos published in the year 2022',
        '9. average duration of all videos in each channel',
        '10. videos with highest number of comments'))


    if question == '1. All the videos and the Channel Name':
        query1=sqldb.select(video.columns.video_name,channel.columns.channel_name)
        resultSet=connection.execute(query1)
        t1=resultSet.fetchall()
        st.write(pd.DataFrame(t1, columns=["Video Name","Channel Name"]))

    elif question == '2. Channels with most number of videos':
        query2=sqldb.select(channel.columns.channel_name,channel.columns.video_count).order_by(sqldb.desc(channel.columns.video_count))
        resultSet=connection.execute(query2)
        t2=resultSet.fetchall()
        st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

    elif question == '3. 10 most viewed videos':
        query3=sqldb.select(channel.columns.channel_name,video.columns.video_name,video.columns.view_count).where(video.columns.view_count!=None).order_by(sqldb.desc(video.columns.view_count))
        resultSet=connection.execute(query3)
        t3=resultSet.fetchall()
        st.write(pd.DataFrame(t3, columns = ["channel Name","video Name","Views"]))

    elif question == '4. Comments in each video':
        query4=sqldb.select(video.columns.video_name,video.columns.comment_count).where(video.columns.comment_count!=None)
        resultSet=connection.execute(query4)
        t4=resultSet.fetchall()
        st.write(pd.DataFrame(t4, columns=["Video Name","No Of Comments"]))

    elif question == '5. Videos with highest likes':
        query5=sqldb.select(channel.columns.channel_name,video.columns.video_name,video.columns.like_count).where(video.columns.like_count != None).order_by(sqldb.desc(video.columns.like_count))
        resultSet=connection.execute(query5)
        t5=resultSet.fetchall()
        st.write(pd.DataFrame(t5, columns=["channel Name","Video Name","Likes"]))

    elif question == '6. likes of all videos':
        query6=sqldb.select(video.columns.video_name,video.columns.like_count).where(video.columns.like_count !=None)
        resultSet=connection.execute(query6)
        t6=resultSet.fetchall()
        st.write(pd.DataFrame(t6, columns=["video Name","Likes"]))

    elif question == '7. views of each channel':
        query7=sqldb.select(channel.columns.channel_name,channel.columns.channel_views).where(channel.columns.channel_views !=None)
        resultSet=connection.execute(query7)
        t7=resultSet.fetchall()
        st.write(pd.DataFrame(t7, columns=["Channel Name","Total Views"]))

    elif question == '8. videos published in the year 2022':
        query8=sqldb.select(channel.columns.channel_name,video.columns.video_name,video.columns.published_date).where(sqldb.func.extract('year',video.columns.published_date)=='2022')
        resultSet=connection.execute(query8)
        t8=resultSet.fetchall()
        st.write(pd.DataFrame(t8,columns=["Channel Name","Video Name","Video Published On"]))

    elif question == '9. average duration of all videos in each channel':
        query9=sqldb.select(video.columns.channel_id,sqldb.func.avg(video.columns.duration).label('Avg_Duration')).group_by(video.columns.channel_id)
        resultSet=connection.execute(query9)
        t9=resultSet.fetchall()
        st.write(pd.DataFrame(t9, columns=['ChannelId', 'Average Duration']))
            
    elif question == '10. videos with highest number of comments':
        query10=sqldb.select(channel.columns.channel_name,video.columns.video_name,video.columns.comment_count).where(video.columns.comment_count !=None).order_by(sqldb.desc(video.columns.comment_count))
        resultSet=connection.execute(query10)
        t10=resultSet.fetchall()
        st.write(pd.DataFrame(t10, columns=['Channel Name',"Video Name", 'NO Of Comments']))