# YOUTUBE-DATA-HARVESTING-AND-WAREHOUSING


## Problem Statement 
The task is to build a Streamlit app that permits users to analyze data from multiple YouTube channels. Users can input a YouTube channel ID to access data like channel information, video details, and user engagement.It allow users to collect data from up to 10 different channels. Additionally, it should offer the capability to migrate selected channel data from the data lake to a SQL database for further analysis. The app should enable searching and retrieval of data from the SQL database, including advanced options like joining tables for comprehensive channel information.

## Technology Stack Used
1. Python
2. MySQL
3. Google Client Library 

## Approach

1. Start by setting up a Streamlit application using the python library "streamlit", which provides an easy-to-use interface for users to enter a YouTube channel ID, view channel details, and select channels to migrate.
2. Establish a connection to the YouTube API V3, which allows me to retrieve channel and video data by utilizing the Google API client library for Python. 
3. Transferring the collected data from multiple channels namely the channels,videos and comments to a SQL data warehouse, utilizing a SQL database like MySQL or PostgreSQL for this purpose.
4. Utilize SQL queries to join tables within the SQL data warehouse and retrieve specific channel data based on user input. For that the SQL table previously made has to be properly given the the foreign and the primary key. 
5. The retrieved data is displayed within the Streamlit application, leveraging Streamlit's data visualization capabilities to create charts and graphs for users to analyze the data.
