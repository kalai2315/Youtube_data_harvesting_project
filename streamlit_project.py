from googleapiclient.discovery import build
import pymongo
import pymysql
import pandas as pd
from urllib.parse import quote
from sqlalchemy import create_engine
import streamlit as st
from streamlit_option_menu import option_menu
import isodate
import random
import time
from isodate import * 

st.set_page_config(page_title="Youtube Data Project", page_icon="🏨", layout="wide")

def setting_bg():
    st.markdown(f""" 
    <style>.stApp {{
        background: url("data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD/2wCEAAkGBwgHBgkIBwgKCgkLDRYPDQwMDRsUFRAWIB0iIiAdHx8kKDQsJCYxJx8fLT0tMTU3Ojo6Iys/RD84QzQ5OjcBCgoKDQwNGg8PGjclHyU3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3Nzc3N//AABEIAJQA/AMBIgACEQEDEQH/xAAaAAADAQEBAQAAAAAAAAAAAAABAgMABAUH/8QAKhAAAgICAgICAgEDBQAAAAAAAAECESExAxJBURNhBCJxYoGhFCMzQlL/xAAZAQADAQEBAAAAAAAAAAAAAAAAAQIDBAX/xAAaEQEBAQEBAQEAAAAAAAAAAAAAARECEiEx/9oADAMBAAIRAxEAPwD6AlY8YfRWPHRVRSOi9PGnCK478D9UlgpH9nVYKLjROtZzCRjKlkbrIdKgiXheiNSCMkB4RLJaK0JWSkXQLkM40TcU0Vk00TlfgQsckl0nb/sMpRclXnY7Wck+Xi65T2abrC859dNYwIssm+RqBuOaWycPYfkaVJkWHkbk7FsuRHV0s1ghNHVXZE58b+h6jrm4iJJFutCuJTPy52hTq+MScEgK81A1WyijkLjQanyn1M6QzJyYFZhJZJyRQEnjA4ippjG6sUonuJXoLXg3GqK0mcr1MaEVQ9AisDJAuQGgUUozQleSUFBo1AcjBdRj2ekbtFOmzg/J5HPldN9fCGVuKv8ANz/xqv5Gf5UOtpZ9HF1syi7H8Ze67Y8rm1aVFJQb+zlg6K/LKCpaYHu/pZJp0RaaZ0fL2pOKz5BKJWpvBYvBqtg60PBZC0vIqNBawUSM4iV5czjkHX6OnogOND1N4Q+P7J8nGy8sIlctBqby52qZqKyiI8DZXlCb+iLLzRJouM+uUwx/gwycUvspGFaFoZuxQTY92IyFWBkzlevIpEdIRNUPFguQaNRrCmCmUcC8lQi5Mojj/Mn2n1WK2LSsyOec5Sm5exHbdlGhlCw1ledThGx8eikY1obqLS8I1TBNp0NyL0SynZcFh0tFJT+iSdjK7KLDOa/67GiCKSd0M9gMUukGLslfsaDA4rQHE3Yd6JU5pIVpDzf0ApnYjNEZWdE1kScM4Gi8uWUSconS4sSUStZXlyuGTdUmVaEZWovJGhaKNAoeo8vZMMY5Z09i8jEohIjJhYc5MEWxkwPD2cn5MY3flluSTSwc05W8hhdT4VDx0KhkLEYZDASGoIeJzRCazReRNoqUrylTRSIetopxwK1HkEMo2iseMWS6uhK8puJolF+2AONYGnyFlou4nO0OngBFZxxoi4nQngnJCOxFxEezocf1JuJSLyg4iThgvWRZRwCLy45xyTcTqnC2TcC5UXhzpMPQsoDdR6nw7wj8kKytE7OOvX8iMhUwjnReR7JZYJzTjaBLROsFFgNijMyVgVgDwN1CsAnyokFmiHYHiMhaLuOR48F5Y0+UOOFsrHjyU4+OsvZVINGE1Gjm51aOmaOXlysDhUkX1wP2snaSyjRkvBSFEgTGiDkyIYMZDJkFhlVIChpC7DNtxtZJJvtkBRlGhFss5L+RXFbQyvJJRsnLjwVyvAdoCxy9DNFnG2zdR6Xl0d7i0hKNFpGbtnNr0/ImbdAegN0IYDkYVvIUy5U3lmjLYTIpOGQaFWxwLBiPHRKUuugLl/pCyk6+KKdNlXjBDgbfHesj9vsCwxlkWzWAw0lcWcslss5CscTY53x2icl1eDonhEV+zvwXqMBSdFIW9m6peB4yoRYV8S3eQ9cBsHZAeEf6rZNvJSbwyYJsGxyayOAw9WK0FWOo+wPE4x3Yeg+A0Gjy41IbsSybJy69S8rqRrTJK6N2KxNijAL2GGWGTDYliOTsepvK9pAfIkRvBksjlTeTzlYeNfJS1YjyP+Mv91ei9Z3l6PBxxjCsvAOTjSja8DQtIM3cGSWOa2ZNg+hurT0AxkOlgSxlICsCaRKSXjA/IyXguIwrlJYApDNEpPqMsVUhE2pE+1bN8i2B4q2no1EY/kXLKOmFSViGFhC3kfqkFL0Oof8AoBgLGhtjKKQ1C0YmohopWABox5doKaOfjfbZVOjPHoaq2urJtAcvCGj9gX6yTHQVVAx4AYwGaxXLIYQN5GixVBt2UXG62B/AborxYonKP2NB0GpvOu2DdptlLOOM60H/AFHhqg1F5XhFdm3so2QhyxY92NNhHtsTvYvLyRi2ibmvDKkTYeU2wJpIk5UrYseRSZReV3LONEeWSu0FypHO207egHk8H2dMZ0lg3HGL0wvj+xWjymoX4OriTSEhA6Yw0vAtHloook2DETdhaPJ1fkdaJpjXgNLBbwI2hZPwiTbvQh5eTco6VDRnJlVE3UNdeJwuy0WzKK9INZFp4Nt+QpVZkhgFJTbD8ebG0ZyHqRX6jKSfkm32wBJRi7YqcabT0zKRBdnOo5K9JemJXyHU/TElLOTfHOtf5A609oBkp4Ta0X+V9TnhS8FHbSocR1A5ZdlRBXFl+qXnJOTd6NGeFm5SjROHaL0WUpegrLyGjE3yP0Tk5SVUdL4ryharwTqvJOKMlVHRGLe2aA3cVo8mVRXsPytk2FAnFEzOdeCLl9mtvyA8q8XK3Onov2tYONbsa37AsUfJU8oRytt+wSnGMf6vRBzd7ASFQTGJdJjUYwgaITGGlhJbMYZVvBHlbRjAIt+IkoOXkuzGGloID44uejGFThukVlIVmMEOpvyKzGKSMRzGEDE5QQDCMltGUnZjAayboKZjASfI2tGi2zGAHQ0NmMCXJNvsxDGAP//Z");
        background-size: cover
    }}
    </style>
    """, unsafe_allow_html=True)

setting_bg()

#establishes a connection to the youtube data API using the API Key.
def Api_connect():
    #Api_Id="AIzaSyDU1mUeLPuCOH9D4Erio4AeNxOMStVNtfE"   
    youtube=build("youtube", "v3", developerKey="AIzaSyDU1mUeLPuCOH9D4Erio4AeNxOMStVNtfE")
    return youtube
youtube=Api_connect()



def get_channel_details(channel_id):
    request= youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id)
                    
    response=request.execute()

    for i in range(len(response["items"])):
        data = dict(Channel_Id=response['items'][i]["id"],
                    Channel_Name=response['items'][i]['snippet']["title"],
                    Subscription_Count=response['items'][i]['statistics']['subscriberCount'],
                    Channel_Views=response['items'][i]['statistics']["viewCount"],
                    Total_videos=response['items'][i]['statistics']['videoCount'],
                    Playlist_Id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Channel_Description=response['items'][i]["snippet"]['description'],
                    Published_At=response['items'][i]["snippet"]['publishedAt']
                    )
    return data


def get_video_ids(channel_id):
    video_ids=[]
    response= youtube.channels().list(
                    part="contentDetails",
                    id=channel_id).execute()
    Playlist_Id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    next_page_token=None
    while True:
        response1= youtube.playlistItems().list(part="snippet",
                                playlistId=Playlist_Id, maxResults=50,pageToken=next_page_token
                                ).execute()
        for i in range(len(response1["items"])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get("nextPageToken")
        if next_page_token is None:
            break
    return video_ids

def get_video_details(video_ids):
    all_videos=[]


    for i in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=i
        )
        response=request.execute()
        for item in response["items"]:
            data=dict(Channel_Name=item["snippet"]["channelTitle"],
                    Channel_Id=item["snippet"]["channelId"],
                    Video_Id=item["id"],
                    Title="".join(item["snippet"].get("title",["NA"])),
                    Tags=",".join(item["snippet"].get("tags",["NA"])),
                    Thumbnail=item["snippet"]["thumbnails"]["default"]["url"],
                    Description=item["snippet"]["description"],
                    Published_Date=item["snippet"]["publishedAt"],
                    Duration=item["contentDetails"]["duration"],
                    ViewCount=item["statistics"]["viewCount"],
                    Likes=item["statistics"].get("likeCount"),
                    Comments=item["statistics"].get("commentCount"),
                    Favourite_Count=item["statistics"]["favoriteCount"],
                    Definition=item["contentDetails"]["definition"],
                    Caption_Status=item["contentDetails"]["caption"]
                                     )
            all_videos.append(data)
    return  all_videos

def get_comment_details(video_ids):
    all_comments=[]
    try:
        for i in video_ids:
            request=youtube.commentThreads().list(
                        part="snippet", videoId=i,
                        maxResults=50) 
            response=request.execute()
            
            for item in response ["items"]:  
                data=dict(Comment_Id=item["snippet"]['topLevelComment']['id'],
                    CommentV_Id=item["snippet"]['topLevelComment']['snippet']['videoId'],
                    Comment_Text=item["snippet"]['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author=item["snippet"]['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_published=item["snippet"]['topLevelComment']['snippet']['publishedAt'])
                all_comments.append(data)
    except:
        pass            
    return all_comments

def get_playlist_details(channel_id):
        All_data=[]
        
        request=youtube.playlists().list(
                                part='snippet,contentDetails',
                                channelId=channel_id,
                                maxResults=50)
                        
        response=request.execute()
        for item in response['items']:
                data=dict(Playlist_Id=item['id'],
                        Playlist_Title=item['snippet']['title'],
                        Channel_Id=item['snippet']['channelId'],
                        Channel_Name=item['snippet']['channelTitle'],
                        PublishedAt=item['snippet']['publishedAt'],
                        Video_Count=item['contentDetails']['itemCount'])
                All_data.append(data)
        return All_data

mongodb_client=pymongo.MongoClient("mongodb+srv://kalaiselviganesan15:1234@cluster0.zxvmivo.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0y")
db=mongodb_client["Youtube_data"]
collection1=db["channel_details"]

def check_channel_exists(channel_id):
    existing_channel = collection1.find_one({'channel_information.Channel_Id': channel_id})
    return existing_channel is not None

def channel_details(channel_id):
    channel_info=get_channel_details(channel_id)
    playlist_info=get_playlist_details(channel_id)
    vid_ids=get_video_ids(channel_id)
    video_info=get_video_details(vid_ids)
    comment_info=get_comment_details(vid_ids)
    collection1.insert_one({"channel_information":channel_info,"playlist_information":playlist_info,"video_information":video_info,"comment_information":comment_info})
    return "successfully uploaded in mongodb"



mydb = pymysql.connect(
        host = "localhost",
        user = "root",
        password = "root",
        
        autocommit = True
    )
mycursor = mydb.cursor()
mycursor.execute("create database if not exists Youtube")
mydb.commit()
mycursor.execute("use Youtube")

from sqlalchemy import create_engine

import pymysql
user = 'root'
password = 'root'
host = 'localhost'
port = 3306
database = 'Youtube'


engine = create_engine(url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database
        ), echo=False)


def data_from_mongodb(channel_id):

    data=collection1.find_one({'channel_information.Channel_Id':channel_id})
    if data is not None:
        channel_data=pd.DataFrame(data["channel_information"],index=[0])
        table_name = 'channels'
        channel_data.to_sql(name=table_name, con=engine, if_exists='append', index=False)

        playlist_data=pd.DataFrame(data["playlist_information"])
        table_name = 'playlists'
        playlist_data.to_sql(name=table_name, con=engine, if_exists='append', index=False)

        video_data=pd.DataFrame(data["video_information"])
        for i in range(len(video_data["Duration"])):
            duration = isodate.parse_duration(video_data["Duration"].loc[i])
            seconds = duration.total_seconds()
            video_data.loc[i, 'Duration'] = int(seconds)
        table_name = 'videos'
        video_data.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        
        comment_data=pd.DataFrame(data["comment_information"])
        if comment_data is not None:
            try:
                table_name = "comments"
                comment_data.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            except:
                print("Comment_data is empty")

def display_introduction(): 
    introduction_markdown = """
# YouTube Data Harvesting and Warehousing

## Introduction

Welcome to the YouTube Data Harvesting and Warehousing Dashboard! This interactive platform allows you to explore and analyze data harvested from YouTube channels, playlists, videos, and comments. The project leverages SQL and MongoDB databases for structured and flexible storage, providing a comprehensive solution for managing YouTube data.

### Key Features

- **Data Harvesting:** Collect valuable insights from YouTube channels, playlists, videos, and user comments.

- **Multi-Database Support:** Utilize both SQL and MongoDB databases for storing and managing different types of data.

- **Streamlit Dashboard:** Visualize and explore the harvested data through an intuitive and user-friendly Streamlit dashboard.

## How to Use

1. **Harvest Data:**
   - Set up your YouTube API keys.
   - Run the data harvesting script to collect information from YouTube.

2. **Database Setup:**
   - Configure your SQL and MongoDB database connections.
   - Run the database setup script to create necessary tables and collections.

3. **Launch Streamlit Dashboard:**
   - Start the Streamlit app to access the interactive dashboard.
   - Explore and analyze data with ease.


---
"""

# Display Introduction Markdown
    st.markdown(introduction_markdown, unsafe_allow_html=True)


page = st.sidebar.radio("📊 YouTube Channel Data Analysis", ["🏠 Home", "📥 Channel Analysis", "🔍 Insights"])


if page== "🏠 Home":
    display_introduction()

if page == "📥 Channel Analysis":
    client=pymongo.MongoClient("mongodb+srv://kalaiselviganesan15:1234@cluster0.zxvmivo.mongodb.net/?retryWrites=true&w=majority")
    db=client["Youtube_data"]
    #collection=db["channel_details"]
    collection1=db["channel_details"]

    def main():
        st.title("Youtube Data Harvesting")

        # Input field for channel ID
        channel_id = st.text_input("Enter Channel ID:", key="channel_id_input")

        # Button to trigger data collection and storage
        if st.button("Collect and Store Data"):
            try:
                # Check if the channel already exists in MongoDB
                if check_channel_exists(channel_id):
                    st.warning("Channel ID already exists in the database.")
                else:
                    # If the channel doesn't exist, insert the channel details into MongoDB
                    insert = channel_details(channel_id)
                    st.success("Data collected and stored in MongoDB successfully!")
            except Exception as e:
                st.error(f"Error occurred: {e}")

        if st.button("Fetch and Transfer Data"):
            if channel_id:
                try:
                    # Attempt to fetch and transfer data
                    data_from_mongodb(channel_id)
                    st.success("Data transfer completed successfully.")
                except Exception as e:
                # Check if the exception indicates that the channel ID already exists
                    if "already exists" in str(e):
                        st.warning("Channel ID already exists in the database.")
                    else:
                        st.error(f"Error occurred: {e}")

  

    def get_channel_names():
        # Retrieve different channel names from the database
        channel_names = []
        cursor = collection1.find({}, {"_id": 0, "channel_information.Channel_Name": 1})#.limit(4)
        for doc in cursor:
            channel_names.append(doc["channel_information"]["Channel_Name"])
        return channel_names


    # retrieves channel data from  SQL database based on the provided channel name.
    def get_channel_data(channel_name):
        query = f"SELECT * FROM channels WHERE Channel_Name = '{channel_name}'"
        channel_data = pd.read_sql(query, engine)
        if not channel_data.empty:
            st.subheader("Channel Information:")
            st.write(channel_data)
        else:
            st.warning("Channel not found. Please enter a valid channel name.")
        return channel_data
    if __name__ == "__main__":
        main()


    def get_video_data(channel_name):
        query = f"SELECT * FROM videos WHERE Channel_Name = '{channel_name}'"
        video_data = pd.read_sql(query, engine)
        if not video_data.empty:
            st.subheader("Video Information:")
            st.write(video_data)
        else:
            st.warning("Video not found. Please enter a valid channel name.")
        return video_data

    def get_playlist_data(channel_name):
        query = f"SELECT * FROM playlists WHERE Channel_Name = '{channel_name}'"
        playlist_data = pd.read_sql(query, engine)
        if not playlist_data.empty:
            st.subheader("Playlist Information:")
            st.write(playlist_data)
        else:
            st.warning("Playlist not found. Please enter a valid channel name.")
        return playlist_data

    def get_comments_data():
            
        comments_data = None
        query = """SELECT *
                FROM comments
                JOIN videos ON comments.CommentV_Id = videos.Video_ID"""
        try:
            comments_data = pd.read_sql(query, engine)
            if not comments_data.empty:
                st.subheader("Comments Information:")
                st.write(comments_data)
            else:
                st.warning("Comments not found. Please enter a valid channel name.")
        
        except Exception as e:
            st.error(f"Error executing SQL query: {e}")
        return comments_data
    


    #def tables_sql():
        #get_channel_data()

    
    def main():
        st.title("YouTube Channel Viewer")

        # Get four different channel names from the database
        channel_names = get_channel_names()

        # User input for channel name using a dropdown
        selected_channel = st.selectbox("Select a Channel:", channel_names)

        # Display button to fetch and show channel data
        if st.button("Show Channel Data"):
            get_channel_data(selected_channel)

        if st.button("Show Video Data"):
            get_video_data(selected_channel)

        if st.button("Show Playlist Data"):
            get_playlist_data(selected_channel)

        if st.button("Show comments Data"):
            #channel_name =(selected_channel)
            get_comments_data()

    if __name__ == "__main__":
        main()



# app.py

if page == "🔍 Insights":
 
    Queries=st.selectbox("SQL Query Output",("Names of all videos and their corresponding channels",
                        "Channels with the most number of videos and the count",
                        "Top 10 most viewed videos and their respective channels",
                        "Number of comments made on each video and their corresponding video names",
                        "Videos with the highest number of likes and their corresponding channel names",
                        "Total number of likes and dislikes for each video and their corresponding video names",
                        "Total number of views for each channel and their corresponding channel names",
                        "Channels that published videos in the year 2022",
                        "Average duration of all videos in each channel",
                        "Videos with the highest number of comments and their corresponding channel names"))

    if Queries=="Names of all videos and their corresponding channels":

        q1='''SELECT Title,Channel_Name from videos;'''
        mycursor.execute(q1)
        mydb.commit()
        data1=mycursor.fetchall()
        st.dataframe(pd.DataFrame(data1,columns=["Title","Channel_Name"]))

    elif Queries=="Channels with the most number of videos and the count":

        q2='''SELECT Channel_Name,Total_videos from channels;'''
        mycursor.execute(q2)
        mydb.commit()
        data2=mycursor.fetchall()
        st.dataframe(pd.DataFrame(data2,columns=["Channel_Name","Total_videos"]))


    elif Queries=="Top 10 most viewed videos and their respective channels":

        q3='''SELECT Channel_Name,Title,ViewCount FROM videos ORDER BY ViewCount DESC LIMIT 10;'''
        mycursor.execute(q3)
        mydb.commit()
        data3=mycursor.fetchall()
        st.dataframe(pd.DataFrame(data3,columns=["Channel_Name","Title","ViewCount"]))

    elif Queries=="Number of comments made on each video and their corresponding video names":

        q4='''SELECT Title,Comments from videos;'''
        mycursor.execute(q4)
        mydb.commit()
        data4=mycursor.fetchall()
        st.dataframe(pd.DataFrame(data4,columns=["Title","ViewCount"]))

    elif Queries=="Videos with the highest number of likes and their corresponding channel names":
        
        q5='''SELECT Channel_Name, Title, MAX(Likes) as max_likes FROM videos GROUP BY Channel_Name, Title
            ORDER BY max_likes DESC;'''
        mycursor.execute(q5)
        mydb.commit()
        data5=mycursor.fetchall()
        st.dataframe(pd.DataFrame(data5,columns=["Channel_Name","Title","max_likes"]))

    elif Queries== "Total number of likes and dislikes for each video and their corresponding video names":

        q6='''SELECT Title,Likes FROM videos;'''
        mycursor.execute(q6)
        mydb.commit()
        data6=mycursor.fetchall()
        st.dataframe(pd.DataFrame(data6,columns=["Title","Likes"]))

    elif Queries== "Total number of views for each channel and their corresponding channel names":

        q7='''SELECT Channel_Name,Channel_Views FROM channels;'''
        mycursor.execute(q7)
        mydb.commit()
        data7=mycursor.fetchall()
        st.dataframe(pd.DataFrame(data7,columns=["Channel_Name","Channel_Views"]))

    elif Queries== "Channels that published videos in the year 2022":

        q8='''SELECT Channel_Name,Title,Published_Date FROM videos WHERE YEAR(Published_Date)=2022;'''
        mycursor.execute(q8)
        mydb.commit()
        data8=mycursor.fetchall()
        st.dataframe(pd.DataFrame(data8,columns=["Channel_Name","Title","Published_Date"]))

    elif Queries== "Average duration of all videos in each channel":

        q9='''SELECT Channel_Name,AVG(Duration) AS avg_duration from videos group by Channel_Name;'''
        mycursor.execute(q9)
        mydb.commit()
        data9=mycursor.fetchall()
        if data9:
            result_df = pd.DataFrame(data9, columns=["Channel_Name", "avg_duration"])
            st.dataframe(result_df)
        else:
            st.warning("No data found for average duration.")
        #st.dataframe(pd.DataFrame(data9,columns=["Channel_Name","avg_duration"]))
    

    elif Queries== "Videos with the highest number of comments and their corresponding channel names":

        q10='''SELECT Channel_Name,Title,Comments from videos order by Comments DESC;'''
        mycursor.execute(q10)
        mydb.commit()
        data10=mycursor.fetchall()
        st.dataframe(pd.DataFrame(data10,columns=["Channel_Name","Title","Comments"]))
