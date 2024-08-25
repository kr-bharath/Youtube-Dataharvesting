# Youtube-Dataharvesting
**Skills take away From This Project**
        ```Python
           MySQL
           Google Client Library
           STEAMLIT APPLICATION```

        
# PROCEDURE:
        
        _Set up a Streamlit Application:_ 
                             Streamlit building data visualization and analysis tools. You can use Streamlit to create a UI where users can enter a YouTube channel ID, view the channel details, and migrate to the data warehouse.
        _
        Connect to the YouTube API_: 
                              Here we  need to use the YouTube API to retrieve channel and video data. Google API client library used  to make requests to the API.
        _
        Store and Clean data_ : 
                             Once the data is retrieved from the YouTube API, store it in a suitable format before migrating to the data warehouse. You can use pandas DataFrames.
        
        _Migrate data to a SQL:_
                             After collected data for multiple channels, you can migrate it to a SQL data warehouse. SQL database MySQL used for this.
        _
        Query the SQL data :_ 
                             SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input Provided. Python SQL library SQLAlchemy to interact with the SQL database.
        _
        Display Query in the Streamlit app: _
                             Finally, displayed the retrieved data in the Streamlit application.
Overall, this approach involves building a UI with Streamlit Application, retrieving data , storing the data to SQL, querying the data with SQL, and displaying the Queries in the Streamlit Application.
