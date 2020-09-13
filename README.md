# Project Intro

This is part of Udacity Data Engineering Nanodegree projects, on data modeling with Cassandra database.
The project is mainly constituted of the notebook `Project_1B_ Project_Template.ipynb` (which content is essentially is the same as the README content below) and a python script `cql_queries.py`.
The python script contains the different CQL queries executed throughout the notebook.

In the 1st section of the notebook csv files are processed and put in a single csv file.
The 2nd section uses CQL queries from `cql_queries.py` script and create tables, populates them and executes queries against these tables.


# Part I. ETL Pipeline for Pre-Processing the Files

#### Import Python packages 


```python
# Import Python packages 
import pandas as pd
import os
import glob
from tqdm import tqdm
import multiprocessing as mp
from cassandra.cluster import Cluster
from cql_queries import *
```


```python
# for printing more rows/columns with pandas
pd.set_option("display.max_rows", 100)
pd.set_option("display.max_columns", None)
```

#### Creating list of filepaths to process original event csv data files


```python
# Folder containing csv files
PATH = os.path.join(os.getcwd(), "event_data")

# List of csv files
list_files = glob.glob(os.path.join(PATH, "*.csv"))
print(f"{len(list_files)} csv files found!")
```

    30 csv files found!



```python
# helper function to load one csv as pandas dataframe
def _read_csv(p): return pd.read_csv(p)
```


```python
%%time

# Load all csv files in parallel and concatenate in single dataframe

with mp.Pool() as pool:
    df_music = pd.concat(
        pool.map(_read_csv, tqdm(list_files)),
        ignore_index=True, copy=False
    )
```

    100%|██████████| 30/30 [00:00<00:00, 43645.20it/s]


    CPU times: user 70.6 ms, sys: 19.6 ms, total: 90.3 ms
    Wall time: 320 ms



```python
print(f"The dataset has {df_music.shape[0]} rows and {df_music.shape[1]} columns") 
```

    The dataset has 8056 rows and 17 columns



```python
print("Sample rows")
df_music.head()
```

    Sample rows





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>artist</th>
      <th>auth</th>
      <th>firstName</th>
      <th>gender</th>
      <th>itemInSession</th>
      <th>lastName</th>
      <th>length</th>
      <th>level</th>
      <th>location</th>
      <th>method</th>
      <th>page</th>
      <th>registration</th>
      <th>sessionId</th>
      <th>song</th>
      <th>status</th>
      <th>ts</th>
      <th>userId</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>NaN</td>
      <td>Logged In</td>
      <td>Kevin</td>
      <td>M</td>
      <td>0</td>
      <td>Arellano</td>
      <td>NaN</td>
      <td>free</td>
      <td>Harrisburg-Carlisle, PA</td>
      <td>GET</td>
      <td>Home</td>
      <td>1.540010e+12</td>
      <td>514</td>
      <td>NaN</td>
      <td>200</td>
      <td>1.542070e+12</td>
      <td>66.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Fu</td>
      <td>Logged In</td>
      <td>Kevin</td>
      <td>M</td>
      <td>1</td>
      <td>Arellano</td>
      <td>280.05832</td>
      <td>free</td>
      <td>Harrisburg-Carlisle, PA</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1.540010e+12</td>
      <td>514</td>
      <td>Ja I Ty</td>
      <td>200</td>
      <td>1.542070e+12</td>
      <td>66.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>NaN</td>
      <td>Logged In</td>
      <td>Maia</td>
      <td>F</td>
      <td>0</td>
      <td>Burke</td>
      <td>NaN</td>
      <td>free</td>
      <td>Houston-The Woodlands-Sugar Land, TX</td>
      <td>GET</td>
      <td>Home</td>
      <td>1.540680e+12</td>
      <td>510</td>
      <td>NaN</td>
      <td>200</td>
      <td>1.542070e+12</td>
      <td>51.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>All Time Low</td>
      <td>Logged In</td>
      <td>Maia</td>
      <td>F</td>
      <td>1</td>
      <td>Burke</td>
      <td>177.84118</td>
      <td>free</td>
      <td>Houston-The Woodlands-Sugar Land, TX</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1.540680e+12</td>
      <td>510</td>
      <td>A Party Song (The Walk of Shame)</td>
      <td>200</td>
      <td>1.542070e+12</td>
      <td>51.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Nik &amp; Jay</td>
      <td>Logged In</td>
      <td>Wyatt</td>
      <td>M</td>
      <td>0</td>
      <td>Scott</td>
      <td>196.51873</td>
      <td>free</td>
      <td>Eureka-Arcata-Fortuna, CA</td>
      <td>PUT</td>
      <td>NextSong</td>
      <td>1.540870e+12</td>
      <td>379</td>
      <td>Pop-Pop!</td>
      <td>200</td>
      <td>1.542080e+12</td>
      <td>9.0</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Save subset of columns of interest in a new csv file

df_music[['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId']].to_csv("event_datafile_new.csv", index=False)
```

# Part II. Complete the Apache Cassandra coding portion of your project. 

## Now we'll load the previously saved csv file and try to insert the data in cassandra tables we'll create. The event_datafile_new.csv contains the following columns: 
- artist 
- firstName of user
- gender of user
- item number in session
- last name of user
- length of the song
- level (paid or free song)
- location of the user
- sessionId
- song title
- userId

The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>

<img src="images/image_event_datafile_new.jpg">


```python
# Load the processed dataset
df_music = pd.read_csv(os.path.join(os.getcwd(), "event_datafile_new.csv"))
```


```python
# Check # of missig values for each column
print("Number of missing values per column")
df_music.isnull().sum()[df_music.isnull().sum() > 0]
```

    Number of missing values per column





    artist       1236
    firstName     286
    gender        286
    lastName      286
    length       1236
    location      286
    song         1236
    userId        286
    dtype: int64



In the above dataframe, some text columns have their missing values encoded as `NaN`. When trying to insert them as is into cassandra tables we will get some errors. To avoid this, we'll fill `NaN` for these colums with empty strings, and user id with `-9999`.
We'll also cast user id to int :


```python
# dictionary for filling NA for string columns with empty string
dict_na = {
    "firstName": "",
    "gender": "",
    "lastName": "",
    "location": "",
    "song": "",
    "artist": "",
    "userId": -9999
}

df_music.fillna(dict_na, inplace=True)
```


```python
df_music["userId"] = df_music.userId.astype(int)
```

#### Creating a Cluster


```python
# Connect to cassandra cluster and create a session

try:
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
except Exception as e:
    print("Can't connect and/or open session")
    print(e)
```

#### Create Keyspace


```python
try:
    session.execute(create_keyspace_query)
except Exception as e:
    print("can't create keyspace")
    print(e)
```

#### Set Keyspace


```python
try:
    session.set_keyspace('big_sparkify')
except Exception as e:
    print("can't set keyspace to session")
    print(e)
```


```python

```

#### Drop all tables if they exist 


```python
for query in list_drop_queries:
    try:
        session.execute(query)
    except Exception as e:
        print(f"can't excute drop table query : {query}")
        print(e)
```

### Given a set of 3 queries we'll create 3 data models accordingly

### The first query :
1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4

We'll create the table `music_play_sessions`, with a primary key composed of 'Session id' and 'item in session', so that we can identify uniquely our rows and reference these fields in WHERE clause.

But first let's verify that these fileds uniquely identify our rows :


```python
print("Number of duplicate rows : ")
print(df_music.duplicated(["sessionId", "itemInSession"]).sum())
```

    Number of duplicate rows : 
    0


Create Table :


```python
try:
    session.execute(create_music_play_sessions)
except Exception as e:
    print(f"Can't execute query {create_music_play_sessions}")
    print(e)
```

Insert data :


```python
%%time

# insert data into 
try:
    for row_item in tqdm(df_music[["sessionId", "itemInSession", "artist", "song", "length", "userId", "firstName", "lastName", "gender", "level", "location"]]\
                         .itertuples(index=False, name=None)):
        session.execute(insert_music_play_sessions, row_item)
except Exception as e:
    print("can't execute insert statement")
    print(e)
```

    8056it [00:10, 748.14it/s]

    CPU times: user 3.28 s, sys: 605 ms, total: 3.88 s
    Wall time: 10.8 s


    


#### Results of query 1


```python
query = """
    SELECT artist, song, length from music_play_sessions WHERE session_id = 338 AND item_in_session = 4
"""

try:
    results = session.execute(select_query1)
    for res in results:
        print(res)
except Exception as e:
    print(e)
```

    Row(artist='Faithless', song='Music Matters (Mark Knight Dub)', length=495.30731201171875)



```python

```

### The second query :
2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

We'll create the table `users_activities`, with a primary key composed of 'user id' and 'Session id' and 'item in session', so that we can identify uniquely our rows and reference 'user id' fields in WHERE clause.

#### Create table :


```python
try:
    session.execute(create_users_activities)
except Exception as e:
    print(f"Can't execute query {create_users_activities}")
    print(e)
```

#### Insert data :


```python
%%time
try:
    for row_item in tqdm(df_music[["userId", "sessionId", "itemInSession", "artist", "song", "length", "firstName", "lastName", "gender", "level", "location"]]\
                         .itertuples(index=False, name=None)):
        session.execute(insert_users_activities, row_item)
except Exception as e:
    print("can't execute insert statement")
    print(e)
```

    8056it [00:10, 769.84it/s]

    CPU times: user 3.26 s, sys: 620 ms, total: 3.88 s
    Wall time: 10.5 s


    


#### Results from query 2 :


```python
try:
    results = session.execute(select_query2)
    for res in results:
        print(res)
except Exception as e:
    print(e)
```

    Row(artist='Down To The Bone', song="Keep On Keepin' On", first_name='Sylvie', last_name='Cruz')
    Row(artist='Three Drives', song='Greece 2000', first_name='Sylvie', last_name='Cruz')
    Row(artist='Sebastien Tellier', song='Kilometer', first_name='Sylvie', last_name='Cruz')
    Row(artist='Lonnie Gordon', song='Catch You Baby (Steve Pitron & Max Sanna Radio Edit)', first_name='Sylvie', last_name='Cruz')



```python

```

### The 3rd query

3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'


Here we'll use song as primary key in addition to session id and item in session (that helps identify uniquely rows). Because some songs are empty strings and primary keys don't accept empty item, we'll filter the rows with empty song when inserting data.

#### Create table :


```python
try:
    session.execute(create_songs_records)
except Exception as e:
    print(f"Can't execute query {create_songs_records}")
    print(e)        
```

#### Insert data :


```python
%%time

try:
    for row_item in tqdm(df_music.loc[df_music.song != "", 
                                      ["song", "sessionId", "itemInSession", "artist", "length", "userId", "firstName", "lastName", "gender", "level", "location"]]\
                         .itertuples(index=False, name=None)):
        session.execute(insert_songs_records, row_item)
except Exception as e:
    print("can't execute insert statement")
    print(e)
```

    6820it [00:08, 822.24it/s] 

    CPU times: user 2.68 s, sys: 568 ms, total: 3.25 s
    Wall time: 8.34 s


    



```python
try:
    results = session.execute(select_query3)
    for res in results:
        print(res)
except Exception as e:
    print(e)
```

    Row(first_name='Sara', last_name='Johnson')
    Row(first_name='Jacqueline', last_name='Lynch')
    Row(first_name='Tegan', last_name='Levine')



```python

```

### Drop the tables before closing out the sessions


```python
for query in list_drop_queries:
    try:
        session.execute(query)
    except Exception as e:
        print(f"can't excute drop table query : {query}")
        print(e)
```


```python

```

### Close the session and cluster connection¶


```python
session.shutdown()
cluster.shutdown()
```


```python

```
