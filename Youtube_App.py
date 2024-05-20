#Required Libraries
import streamlit as st
import googleapiclient.discovery
import math
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from googleapiclient.errors import HttpError
import pandas as pd
import re
from sqlalchemy import exc
import numpy as np
import json
import sys
from streamlit_option_menu import option_menu
from PIL import Image
from mysql.connector import errorcode

#Necessary Api connection
api_service_name = "youtube"
api_version = "v3"
ak='AIzaSyBswIuWIwJVBH4fR_YxyKm4LQ5QPy7IKYs'
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=ak)

#function to retrieve channel information
def channel_details(cid): 
        try:   
            #Get channel data from API response
            requestC = youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=cid
            )
            responseC = requestC.execute()
            ChannelData = {
                    'channel_name' : responseC['items'][0]['snippet']['title'],
                    'channel_id' : responseC['items'][0]['id'],
                    'playlist_id' : responseC['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                    'channel_description' : responseC['items'][0]['snippet']['description'],
                    'subscription_count' : responseC['items'][0]['statistics']['subscriberCount'],
                    'channel_views' : responseC['items'][0]['statistics']['viewCount']
                    }
        except KeyError as e:
            st.error("Please check if correct Channel id is entered.")
            sys.exit()
        except Exception as e:
            st.write("Channel details retrieve failed.", e)
            sys.exit() 
        return ChannelData

#Function to loop through Playlist tokens and get Video id details
def Video_playlist_token_details(cid):
            try:
                #Get Playlist Id for the channel id passed
                requestCv = youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=cid
                )
                responseCv = requestCv.execute()
                ChannelPlayListID = responseCv['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                #If channel data available loop through playlist items to get all the Videos
                if responseCv['items']:
                    requestplaylistItems = youtube.playlistItems().list(
                            part="snippet",
                            maxResults="50",
                            playlistId=ChannelPlayListID
                    )
                    responseplaylistItems = requestplaylistItems.execute()
                    #Calculate the total number of pages
                    TotPage = math.ceil( responseplaylistItems['pageInfo']['totalResults']/responseplaylistItems['pageInfo']['resultsPerPage'])
                    FirstPagevideo = []
                    NextPagevideo = []
                    LastPageVideo = []
                    if TotPage < 2:
                            PageToken = 0
                    else:
                            PageToken = responseplaylistItems['nextPageToken'] 
                    #Loop through each page token till last page and store the videos
                    for i in range(TotPage):
                            if i == 0:
                                for j in range(len(responseplaylistItems['items'])):
                                    videoId=[]
                                    videoId = responseplaylistItems['items'][j]
                                    FirstPagevideo.append(videoId)
                            elif i > 0 and i <= (TotPage-2):
                                    requestNextPageVideos = youtube.playlistItems().list(
                                            part="snippet,contentDetails",
                                            maxResults="50",
                                            pageToken=PageToken,
                                            playlistId=ChannelPlayListID
                                            )
                                    responseNextPageVideos = requestNextPageVideos.execute()
                                    PageToken = responseNextPageVideos['nextPageToken']
                                    for k in range(len(responseNextPageVideos['items'])):
                                            NextPageVideoId=[]
                                            NextPageVideoId = responseNextPageVideos['items'][k]
                                            NextPagevideo.append(NextPageVideoId) 
                            elif i == (TotPage-1):
                                    requestLastPageVideos = youtube.playlistItems().list(
                                            part="snippet,contentDetails",
                                            maxResults="50",
                                            pageToken=PageToken,
                                            playlistId=ChannelPlayListID
                                            )
                                    responseLastPageVideos = requestLastPageVideos.execute()
                                    for l in range(len(responseLastPageVideos['items'])):
                                            LastPageVideoId=[]
                                            LastPageVideoId = responseLastPageVideos['items'][l]
                                            LastPageVideo.append(LastPageVideoId) 
                    #Append all the videos
                    TotalVideos = []
                    TotalVideos = FirstPagevideo + NextPagevideo + LastPageVideo
            except Exception as e:
                st.error("Pagetoken retrieval failed.",e)
                sys.exit()
            return TotalVideos

#Function to get video and comment details in nested Json format
def video_comment_details(TotalVideos):
    try:              
        VideosAndComment=[]
        #Loop through each items in total videos list to get video and comment information
        for item in TotalVideos: 
            VideoId = item['snippet']['resourceId']['videoId']
            Requestvideo = youtube.videos().list(
                    part='snippet,statistics,contentDetails',
                    id=VideoId
                    )
            Responsevideo = Requestvideo.execute()
            #If video data available collect the necessary video data
            if Responsevideo['items']:
                    Vi  = {
                            "video_id": VideoId,
                            "playList_id" : item['snippet']['playlistId'],
                            "channel_id": Responsevideo['items'][0]['snippet']['channelId'],
                            "video_name": Responsevideo['items'][0]['snippet']['title'],
                            "video_description" : Responsevideo['items'][0]['snippet']['description'] ,
                            "video_published_at" :  Responsevideo['items'][0]['snippet']['publishedAt'],
                            "view_count" : Responsevideo['items'][0]['statistics'].get('viewCount',0),
                            "like_count": Responsevideo['items'][0]['statistics'].get('likeCount',0),
                            "comment_count" : Responsevideo['items'][0]['statistics'].get('commentCount',0),
                            "favourite_count" : Responsevideo['items'][0]['statistics'].get('favoriteCount',0),
                            "thumbnails": Responsevideo['items'][0]['snippet']['thumbnails']['default']['url'],
                            "duration_in_seconds" : Responsevideo['items'][0]['contentDetails']['duration'],
                            "comment" : []
                            }
                    try:
                        #Based on the collected VideoID run the comment request.
                        requestComment = youtube.commentThreads().list(
                               part="snippet",
                                videoId=Vi['video_id'],
                                maxResults="100"
                                )
                        responseComment = requestComment.execute()
                        for comment in responseComment['items']:
                                comment_information = {
                                        "comment_id": comment['snippet']['topLevelComment']['id'],
                                        "video_id" : comment['snippet']['videoId'],
                                        "comment_text": comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                                        "comment_author": comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                        "comment_published_at": comment['snippet']['topLevelComment']['snippet']['publishedAt']
                                        }
                                Vi['comment'].append(comment_information)   
                    except Exception as e:
                        pass
                                       
                    VideosAndComment.append(Vi)
    except Exception as e:    
        st.write("Failed to retrieve video details.", e)
        sys.exit() 
    return VideosAndComment 

#Function to call all the Data retrieval functions
def retrieve_data(cid):
   Information={}
   Information['ChannelInfos'] = channel_details(cid)
   TotalVideos = Video_playlist_token_details(cid) 
   Information['VideoInfos'] = video_comment_details(TotalVideos)
   return Information

def transform_data(Information):
    try:
        #Json format transformation into dataframe using pandas json normalise
        json_string = json.dumps(Information, indent=4) 
        ChannelDataFrame=pd.json_normalize(Information['ChannelInfos'])
        ChannelDataFrame.replace('', np.nan, inplace=True)
        VideoDataFrame=pd.json_normalize(Information['VideoInfos'], meta=['comment'])
        VideoDataFrame = VideoDataFrame.drop('comment', axis=1)
        VideoDataFrame.replace('', np.nan, inplace=True)
        VideoDataFrame['video_published_at'] = pd.to_datetime(VideoDataFrame['video_published_at']).dt.tz_localize(None)   
        #Regular expression to match the duration format
        DurationPattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'
        #Function to format duration strings
        def format_duration(DurationStr):
            DurationMatch = re.match(DurationPattern, DurationStr)
            if DurationMatch:
                hours = int(DurationMatch.group(1)) if DurationMatch.group(1) else 0
                minutes = int(DurationMatch.group(2)) if DurationMatch.group(2) else 0
                seconds = int(DurationMatch.group(3)) if DurationMatch.group(3) else 0
                TotalSeconds = hours * 3600 + minutes * 60 + seconds
                return TotalSeconds
            else:
                return None
        #Apply the function to each element in the Duration column
        VideoDataFrame['duration_in_seconds'] = VideoDataFrame['duration_in_seconds'].apply(lambda x: format_duration(x))
        #Get the comment data for all the videos
        RowData=[]
        for i in range (len(Information['VideoInfos'])):
           CommentRowData=pd.json_normalize(Information['VideoInfos'][i]['comment']) 
           RowData.append(CommentRowData)
        CommentDataFrame=pd.concat(RowData, ignore_index=True)
        CommentDataFrame.replace('', np.nan, inplace=True)
        CommentDataFrame['comment_published_at'] = pd.to_datetime(CommentDataFrame['comment_published_at']).dt.tz_localize(None)
    except Exception as e:
        st.write("Data transformation failed:", e)
        sys.exit()
    return ChannelDataFrame, VideoDataFrame, CommentDataFrame

def check_for_channel_id(channel_id):
    try:
        #Connect MySQL
        mydb = mysql.connector.connect(
                                    host="localhost",
                                    user="root",
                                    password="root",
                                    database="youtube"
                                )
        mycursor = mydb.cursor()
        #Select query to search for if user entered channel id is available in Channel table or not
        SelectQuery = "SELECT channel_name,channel_id FROM channel WHERE channel_id=%s"
        ChannelID = channel_id
        mycursor.execute(SelectQuery, (ChannelID,))
        FetchedRows = mycursor.fetchall()
        #Chek if Data is fetched for the entered channel ID
        if FetchedRows:
            df = pd.DataFrame(FetchedRows,columns=['Channel Name','Channel ID']).reset_index(drop=True)
            df.index += 1
            st.error('Oops!! Entered Channel ID data is already available. Please enter another Channel ID..')
            st.table(df)
            sys.exit()
    #Main purpose is to pass if table is not available when the script runs for the first time
    except mysql.connector.ProgrammingError as err:
        pass            
    except Exception as e:
        st.write("Channel ID exist check failed:", e)
        sys.exit()
    return None

#Streamlit configurations

# SETTING PAGE CONFIGURATIONS
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing - Mamtha S")

#set the colour of sidebar
st.markdown("""
<style>
    [data-testid=stSidebar] {
        background-color: #FFA07A;
    }
    .sidebar-title {
    font-weight: bold; /* Makes the text bold */
    color: #333; /* Sets the text color to a dark shade (e.g., black) */
}

</style>
""", unsafe_allow_html=True)

#set the option menu's option list and customise styles
st.sidebar.title(" Youtube Data Harvesting and Datawarehousing")
with st.sidebar:
    option = option_menu(None,
                #menu_title='Main Menu',
                options=["Home", 'Retrieve/Migrate','Queries'],
                icons=['house', 'cloud-upload','question'],
                menu_icon="cast",
                default_index=0,
                styles={
                    "container": {"background-color":'white',"height":"188px","border": "3px solid #000000","border-radius": "0px"},
        "icon": {"color": "black", "font-size": "16px"}, 
        "nav-link": {"color":"black","font-size": "15px", "text-align": "centre", "margin":"4px", "--hover-color": "white","border": "1px solid #000000", "border-radius": "10px"},
        "nav-link-selected": {"background-color": "#FFA07A"},}
                
                )
    
#Set the details in Home page
if option == 'Home':   
    st.divider()
    st.markdown("<h3 style='text-align: center;'><strong>Youtube Data Harvesting and Datawarehousing</strong></h3>", unsafe_allow_html=True)
    st.divider()
    st.markdown("<h4 style='text-align: left;'><strong>Problem Statement</strong></h4>", unsafe_allow_html=True)
    st.markdown('''The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.
                The application should have the Ability to input a YouTube channel ID and retrieve all the relevant data, Option to store the data in a MYSQL 
                and Ability to search and retrieve data from the SQL database using different search options. ''')
    st.markdown("<h4 style='text-align: left;'><strong>Skills take away:</strong></h4>", unsafe_allow_html=True)
    multi = ''' 
        1. Python scripting
        2. Data Collection
        3. Streamlit
        4. Data Management using SQL'''
    st.markdown(multi)
    


#Retrieve and Migrate data to Sql in the second page
if option == 'Retrieve/Migrate':
    st.markdown("<h3 style='text-align: center;'>Retrieve/Migrate data</h3>", unsafe_allow_html=True)
    #Set the session state variables for Dataframes
    if 'ChannelDataFrame' not in st.session_state:
        st.session_state['ChannelDataFrame'] = None
    if 'VideoDataFrame' not in st.session_state:
        st.session_state['VideoDataFrame'] = None
    if 'CommentDataFrame' not in st.session_state:
        st.session_state['CommentDataFrame'] = None


    channel_id = st.text_input("Enter Channel Id:", key='placeholder')
    tab1,tab2 = st.tabs(["RETRIEVE","MIGRATE"])
    #In the Retrieve tab give the button to retrieve channel data
    with tab1:
        st.markdown("<h5>Retrieve and Transform Channel details</h5>", unsafe_allow_html=True)
        st.write("To retrieve data click the below button:point_down:")
        Get_channel_id = st.button('Retrieve')
        
        if (Get_channel_id):
            
            with st.spinner("Data retrieving is in progress..."):
                check_for_channel_id(channel_id)
                Information = retrieve_data(channel_id)
                st.session_state['ChannelDataFrame'], st.session_state['VideoDataFrame'], st.session_state['CommentDataFrame'] = transform_data(Information)
            st.success("Data retrieved and transformed successfully. Move to the Migrate tab.")
    #In the Migrate tab give the button to migrate data from dataframe to Sql
    with tab2:
        st.markdown("<h5>Migrate the transformed data to sql</h5>", unsafe_allow_html=True)
        st.write("To migrate data click the below button:point_down:")
        Store = st.button('Migrate to SQL')
        if Store:
            if st.session_state['ChannelDataFrame'] is not None and st.session_state['VideoDataFrame'] is not None and st.session_state['CommentDataFrame'] is not None:

                #Storing data in SQL
                st.write()
                with st.spinner("Storing data in SQL..."):
                    try:
                        #Connect to MySQL database
                        mydb = mysql.connector.connect(
                            host="localhost",
                            user="root",
                            password="root",
                            database="youtube"
                        )
                        mycursor = mydb.cursor()
                        #Create SQLAlchemy engine
                        engine = create_engine("mysql+mysqlconnector://root:root@localhost/youtube")
                        #Create tables if not exist
                        mycursor.execute("""
                            CREATE TABLE IF NOT EXISTS Channel (
                                channel_name VARCHAR(255),
                                channel_id VARCHAR(255) ,
                                playlist_id VARCHAR(255),
                                channel_description TEXT,
                                subscription_count INT,
                                channel_views BIGINT, 
                                PRIMARY KEY (channel_id,playlist_id))
                            """)
                        mycursor.execute("""
                            CREATE TABLE IF NOT EXISTS Video (
                                video_id VARCHAR(255) PRIMARY KEY,
                                playList_id VARCHAR(255),
                                channel_id VARCHAR(255),
                                video_name VARCHAR(255),
                                video_description TEXT,
                                video_published_at DATETIME,
                                view_count BIGINT,
                                like_count BIGINT,
                                comment_count INT,
                                favourite_count BIGINT,
                                thumbnails VARCHAR(255),
                                duration_in_seconds INT,
                                FOREIGN KEY (channel_id,playList_id)  REFERENCES Channel(channel_id,playList_id) )
                            """)
                        mycursor.execute("""
                            CREATE TABLE IF NOT EXISTS Comment (
                                comment_id VARCHAR(255) PRIMARY KEY,
                                video_id VARCHAR(255),
                                comment_text TEXT,
                                comment_author VARCHAR(255),
                                comment_published_at DATETIME,
                                FOREIGN KEY (video_id)  REFERENCES Video(video_id))
                            """)
                        # Insert data into tables
                        st.session_state.ChannelDataFrame.to_sql(name="Channel", if_exists='append', con=engine, index=False)
                        st.session_state.VideoDataFrame.to_sql(name='Video', con=engine, if_exists='append', index=False)
                        st.session_state.CommentDataFrame.to_sql(name='Comment', con=engine, if_exists='append', index=False)
                        # Commit changes
                        mydb.commit()
                    except exc.IntegrityError as e:
                        st.error("Entered Channel id details are already available in SQL. Please enter different Channel id")
                        mydb.rollback()
                        sys.exit()
                    except Exception as e:
                        st.write("An error occurred while storing data in SQL:", e)
                        mydb.rollback()
                        sys.exit()
                st.success("Data stored in SQL successfully.")

if option == "Queries":
    st.subheader(':black[Ask your Queries]')
    # Selectbox creation
    question_tosql = st.selectbox('Select your Question:',
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
    mydb = mysql.connector.connect(
                        host="localhost",
                        user="root",
                        password="root",
                        database="youtube"
                    )
    mycursor = mydb.cursor()
    # Question1
    if question_tosql == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute('''SELECT  b.channel_name ,a.video_name FROM video as a
                            INNER JOIN channel as b ON a.playlist_id = b.playlist_id;
               ''')
        result_1 = mycursor.fetchall()
        df1 = pd.DataFrame(result_1, columns=['Channel Name','Video Name' ]).reset_index(drop=True)
        df1.index += 1
        st.dataframe(df1)

    # Question2
    if question_tosql == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute('''SELECT a.channel_name,count(b.video_id) as cov FROM channel as a
                        INNER JOIN video as b ON a.playlist_id = b.playlist_id 
                        group by 1 order by cov desc limit 1;
               ''')
        result_1 = mycursor.fetchall()
        df1 = pd.DataFrame(result_1, columns=[ 'Channel Name','Video Count']).reset_index(drop=True)
        df1.index += 1
        st.dataframe(df1)

    # Question3
    if question_tosql == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute('''SELECT a.channel_name,b.video_name,b.view_count FROM channel as a
                            INNER JOIN video as b ON a.playlist_id = b.playlist_id
							order by b.view_count desc limit 10;
               ''')
        result_1 = mycursor.fetchall()
        df1 = pd.DataFrame(result_1, columns=['Channel Name','Video Name','View Count' ]).reset_index(drop=True)
        df1.index += 1
        st.dataframe(df1)


    # Question4
    if question_tosql == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute('''SELECT b.video_name,b.video_id,count(a.comment_id) as cov FROM Comment as a
                            INNER JOIN video as b ON a.video_id  = b.video_id
							group by 1,2 order by cov desc;
               ''')
        result_1 = mycursor.fetchall()
        df1 = pd.DataFrame(result_1, columns=['Video Name','Video ID','Comment Count']).reset_index(drop=True)
        df1.index += 1
        st.dataframe(df1)

    # Question5
    if question_tosql == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute('''SELECT a.channel_name,b.video_name  FROM channel as a
                            INNER JOIN video as b ON a.playList_id  = b.playList_id 
                            where b.like_count = (SELECT max(a.like_count) as mlc from  video as a  )
               ''')
        result_1 = mycursor.fetchall()
        df1 = pd.DataFrame(result_1, columns=[ 'Channel Name','Video Name']).reset_index(drop=True)
        df1.index += 1
        st.dataframe(df1)

    # Question6
    if question_tosql == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute('''SELECT video_name,like_count FROM video 
                            order by like_count desc;
               ''')
        result_1 = mycursor.fetchall()
        df1 = pd.DataFrame(result_1, columns=['Video Name','Video Like Count' ]).reset_index(drop=True)
        df1.index += 1
        st.dataframe(df1)


    # Question7
    if question_tosql == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute(''' SELECT b.channel_id, b.channel_name,b.channel_views as cov FROM channel as b
                             order by cov desc;
               ''')
        result_1 = mycursor.fetchall()
        df1 = pd.DataFrame(result_1, columns=[ 'Channel Id','Channel Name','Total Channel View ' ]).reset_index(drop=True)
        df1.index += 1
        st.dataframe(df1)

    # Question8
    if question_tosql == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute('''SELECT distinct a.channel_name FROM channel  as a
                            INNER JOIN video as b ON b.playList_id = a.playList_id
                            where year(b.video_published_at) =2022;
               ''')
        result_1 = mycursor.fetchall()
        df1 = pd.DataFrame(result_1, columns=['Channel Name' ]).reset_index(drop=True)
        df1.index += 1
        st.dataframe(df1)

    # Question9
    if question_tosql == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute('''SELECT b.channel_name, avg(duration_in_seconds) as DurationInSeconds  FROM video as a
                            INNER JOIN channel as b ON a.playList_id = b.playList_id
                            group by 1;
               ''')
        result_1 = mycursor.fetchall()
        df1 = pd.DataFrame(result_1, columns=['Channel Name','Average Duration In Sec' ]).reset_index(drop=True)
        df1.index += 1
        st.dataframe(df1)

    # Question10
    if question_tosql == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute('''SELECT a.channel_name, b.video_name,c.mlc FROM channel as a
                            INNER JOIN video as b ON a.playList_id = b.playList_id 
                            INNER JOIN (
                                        SELECT a.video_id, count(a.comment_id) as mlc FROM comment as a 
                                        GROUP BY a.video_id ORDER BY mlc DESC
                                        ) as c ON b.video_id = c.video_id
                            WHERE c.mlc = (SELECT max(mlc) FROM (
                                           SELECT count(comment_id) as mlc FROM comment GROUP BY video_id
                                        ) as max_counts); ''')
        result_1 = mycursor.fetchall()
        df1 = pd.DataFrame(result_1, columns=[ 'Channel Name','Video Name','Comment Count']).reset_index(drop=True)
        df1.index += 1
        st.dataframe(df1)
