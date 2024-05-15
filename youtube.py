import streamlit as st
import googleapiclient.discovery
import pandas as pd
#to get api connect
def api_connect():
    api_service_name = "youtube"
    api_version = "v3"
    api_key="AIzaSyC_2SUZR0p9IDEeOwlqN-IbH9SdTbT71X4"
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    return youtube
youtube=api_connect()
#to get channel_details
def get_channel_details(channel_id):
    request = youtube.channels().list( part="snippet,contentDetails,statistics", id=channel_id)
    response = request.execute()
    for i in response['items']:
        channel_data=dict(
                channel_id=i['id'],
                channel_name=i['snippet']["title"],
                channel_des=i['snippet']["description"],
                channel_ply=i["contentDetails"]["relatedPlaylists"]["uploads"],
                channel_v=i['statistics']['viewCount'],
                channel_vdo=i['statistics']['videoCount'],
                channel_sub=i['statistics']['subscriberCount'],
                )
    a=channel_data.values()      
    return a
 # to get vd_id:
def get_vd_ids(channel_id):
    vd_id=[]
    request = youtube.channels().list( part="contentDetails",id=channel_id)
    response = request.execute()
    playlists_id=response['items'][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    next_page_token=None
#to get vd_id
    while True:
        request = youtube.playlistItems().list(part="snippet",playlistId=playlists_id,maxResults=50,pageToken=next_page_token)
        response1 = request.execute()
        for i in range(len(response1['items'])):
            vd_id.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')
        if next_page_token is None:
           break
    return vd_id     
#to get vd info
def get_vd_info(vd_id):
    for i in vd_id:
        request = youtube.videos().list(part="snippet,contentDetails,statistics",id=i)
        response = request.execute()

        for v in response['items']:
            video=dict(
                Channel_id=v['snippet']['channelId'],
                Channel_title=v['snippet']['channelTitle'],
                Video_id=v['id'],
                Title=v['snippet']['title'],
                Description=v['snippet']['description'],
                Published_date=v['snippet']['publishedAt'],
                Thumbnails=v['snippet']['thumbnails']['default']['url'],
                Duration=v['contentDetails']['duration'],
                Caption=v['contentDetails']['caption'],
                Likecount=v['statistics'].get('likeCount'),
                viewcount=v['statistics'].get('viewCount'),
                Favoritecount=v['statistics']['favoriteCount'],
                Commentcount=v['statistics'].get('commentCount')
            )
        b=video.values()
    return b  
#to get comment details
def get_cmd_details(vd_id):
    try:
    
       for j in vd_id:
        request = youtube.commentThreads().list(part="snippet",videoId=j,maxResults=50)
        response = request.execute()

        for c in response['items']:
            comment=dict(
                comment_id=c['snippet']['topLevelComment']['id'],
                video_id=c['snippet']['topLevelComment']['snippet']['videoId'],
                Text_display=c['snippet']['topLevelComment']['snippet']['textDisplay'],
                Author_name=c['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                published_at=c['snippet']['topLevelComment']['snippet']['publishedAt']
            )
        cm=comment.values()
        return cm   
    except:
        pass     

#to get all details
def channeldetails(channel_id):
    global a
    global b
    global cm
    a=get_channel_details(channel_id)
    v=get_vd_ids(channel_id)
    b=get_vd_info(v)
    cm=get_cmd_details(v)
    return "succesfully get channel details"  


#sql
import mysql.connector
mydb = mysql.connector.connect(host="localhost",user="root",password="",database="youtube")
print(mydb)
mycursor = mydb.cursor(buffered=True) 

#streamlit 
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Takeaway")
    st.caption("Python Scripting")
    st.caption("API Integration")
    st.caption("DataBase Mangement Using Sql")

channel_id=st.text_input("Enter the Channel Id")  
if st.button("collecting data"):
    if channel_id:
            details=channeldetails(channel_id)
            st.success(details)
             
    
if st.button("Insert to sql"):
    if channel_id:
            details=channeldetails(channel_id)
    def channel_table(a):
        mycursor.execute("create table if not exists youtube.channel(channelID varchar(250),channelname varchar(250),Description text,playlistid varchar(250),viewcount int,videocount int,subscriptioncount int)")
        sql=("insert into youtube.channel(channelID,channelname,Description,playlistid,viewcount,videocount,subscriptioncount)values(%s,%s,%s,%s,%s,%s,%s)")
        c=tuple(a)
        mycursor.execute(sql,c)  
        mydb.commit()
#create table for video  
    def video_table(b):
        mycursor.execute("create table if not exists youtube.video(channelID varchar(255),Channel_Title text,videoID varchar(255),vd_title text,Description text,Published_at datetime,Thumbanails varchar(255),duration varchar(255),caption varchar(255),likecount bigint,viewcount bigint,favouritecount bigint,commentcount bigint)")
        sql=("insert into youtube.video(channelID,Channel_Title,videoID,vd_title,Description,Published_at,Thumbanails,duration,caption,likecount,viewcount,favouritecount,commentcount)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        v=tuple(b)
        mycursor.execute(sql,v)
        mydb.commit()    
#create table for comment
    def comment_table(cm):
        mycursor.execute("create table if not exists youtube.comment(comment_id varchar(255),video_id varchar(255),comment_text text,Author varchar(255),Published_at datetime)")
        sql=("insert into youtube.comment(comment_id,video_id,comment_text,Author,Published_at)values(%s,%s,%s,%s,%s)")
        co=tuple(cm)
        mycursor.execute(sql,co)
        mydb.commit()

    channel_table(a)    
    video_table(b)
    comment_table(cm)
    st.success("successfully migrated to sql")

questions = st.selectbox('Questions',
    ['Click the question that you would like to query',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
if questions == '1. What are the names of all the videos and their corresponding channels?':
    query1=("select vd_title as vd_names,Channel_Title as channel_name from youtube.video ORDER BY Channel_Title")
    mycursor.execute(query1)
    mydb.commit()
    t1=mycursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

  
elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
    query2=("select channelname as CHANNEL_NAME,videocount as TOTAL_VIDEOS from youtube.channel ORDER BY videocount DESC")
    mycursor.execute(query2)
    mydb.commit()
    t2=mycursor.fetchall()
    df=pd.DataFrame(t2,columns=["CHANNEL_NAME", "TOTAL_VIDEOS"])
    st.write(df)

elif questions == '3. What are the top 10 most viewed videos and their respective channels?': 
    query3=("select viewcount as VIEW,Channel_Title as CHANNELS,vd_title as VIDEO_TITLE from youtube.video ORDER BY viewcount DESC limit 10")   
    mycursor.execute(query3)
    mydb.commit()
    t3=mycursor.fetchall()
    df=pd.DataFrame(t3,columns=["VIEW","CHANNELS","VIDEO_TITLE"])
    st.write(df)

elif questions == '4. How many comments were made on each video, and what are their corresponding video names?': 
    query4=("select videoID as VD_ID,vd_title as VD_TITLE,commentcount as COMMENTS from youtube.video")
    mycursor.execute(query4)
    mydb.commit()
    t4=mycursor.fetchall()
    df=pd.DataFrame(t4,columns=["VD_ID","VD_TITLE","Total_comments"])   
    st.write(df)

elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?': 
    query5=("select Channel_Title as CHANNEL_NAMES,vd_title as TITLE,likecount as LIKES from youtube.video ORDER BY likecount DESC")   
    mycursor.execute(query5)
    mydb.commit()
    t5=mycursor.fetchall()
    df=pd.DataFrame(t5,columns=["CHANNEL_NAME","TITLE","LIKES"])
    st.write(df)

elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    query6=("select likecount as LIKES,vd_title as VIDEO_NAME from youtube.video")
    mycursor.execute(query6)
    mydb.commit()
    t6=mycursor.fetchall()
    df=pd.DataFrame(t6,columns=["LIKES","VIDEO_NAME"])
    st.write(df)

elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?': 
    query7=("select channelname as CHANNEL_NAME,viewcount as VIEW_COUNT from youtube.channel")
    mycursor.execute(query7)
    mydb.commit()
    t7=mycursor.fetchall()
    df=pd.DataFrame(t7,columns=["CHANNEL_NAME","VIEW_COUNT"])
    st.write(df)   
                    
elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':  
    query8=('''select Channel_Title as CHANNEL_NAME,published_at as PUBLISHED_DATE
            from youtube.video 
            WHERE YEAR(published_at)=2022''')  
    mycursor.execute(query8)
    mydb.commit()
    t8=mycursor.fetchall()
    df=pd.DataFrame(t8,columns=["CHANNEL NAME","PUBLISHED_DATE"]) 
    st.write(df)               

elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query9=('''SELECT Channel_Title,
            duration,
            CASE
                WHEN duration REGEXP '^PT[0-9]+M[0-9]+S$' THEN 
                    SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'T', -1) * 60 +
                    SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1)
                WHEN duration REGEXP '^PT[0-9]+S$' THEN 
                    SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'T', -1)
            END AS duration_seconds
            FROM youtube.video''')
    mycursor.execute(query9)
    mydb.commit()
    t9=mycursor.fetchall()
    df=pd.DataFrame(t9,columns=mycursor.column_names)
    st.write(df)

elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    query10=("select Channel_Title as CHANNEL_NAME,commentcount as COMMENT_COUNT from youtube.video ORDER BY commentcount DESC")
    mycursor.execute(query10)
    mydb.commit()
    t10=mycursor.fetchall()
    df=pd.DataFrame(t10,columns=mycursor.column_names)
    st.write(df)