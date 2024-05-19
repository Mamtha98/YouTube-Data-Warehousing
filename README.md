# YouTube-Data-Warehousing-using-SQL-and-Streamlit

# Problem Statement
The goal of this project is to develop a Streamlit application that enables users to analyze data from multiple YouTube channels. Users can input a YouTube channel ID to access various data such as channel information, video details, and comment information. The application should support data collection from up to 10 different channels and store the collected data in a MySQL database.

# Features:
1. Data Collection: Users can input a YouTube channel ID to retrieve channel information, video details, and comment information. The app will fetch data from the YouTube Data API.
2. Data Migration: Data collected from YouTube channels will be migrated to the MySQL database, ensuring data persistence and organization.
3. Search and Retrieval: The app should enable searching and retrieval of data from the SQL database, including advanced options like joining tables for comprehensive channel information.

# Technology Stack Used
1. Streamlit
2. YouTube Data API
3. MySQL
4. Python Scripting

# Approach
1. Begin by setting up a Streamlit application using the Python library "streamlit," which offers a user-friendly interface for users to input a YouTube channel ID, access channel details, and select channels for migration.
2. Establish a connection to the YouTube API V3 using the Google API client library for Python, enabling the retrieval of channel and video data.
3. Create a function to fetch data from the Channel, Video, and Comment APIs and format it into JSON format. Utilize the JSON normalize function to flatten the nested dictionary, enabling the transformation necessary for storing the data into three distinct DataFrames.
4. Transfer the dataframe data to a SQL data warehouse, leveraging the SQLAlchemy library to import data from dataframes into the MySQL database.
5. Utilize SQL queries to perform table joins within the SQL data warehouse and retrieve specific channel data based on user input.
6. Display the retrieved data within the Streamlit application.
