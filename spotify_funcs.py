"""
The following is a series of functions used to create a dataframe containing user listening information with
expanded columns from the Spotify API, and another series used to clean the resulting dataframe for the purposes
of analysis.

Authors: Katy Mombourquette, David Mombourquette

All that is required to run these is to open a blank notebook, import the libraries
below, and run the final function get_spotify_data(). Running the cleaning and pivot functions
is optional, the user may wish to clean the data themselves for their own purposes.

For example:

		from spotify_funcs import *
		import numpy as np
		import pandas as pd
		import os


		# default params = mine

		df = get_spotify_data()
"""


# Make sure to import these in your Spotify program
import pandas as pd
import numpy as np
import os, requests, base64
import re
import ast
import json
from sklearn.metrics.pairwise import euclidean_distances
from sklearn.preprocessing import OneHotEncoder
import streamlit as st
from dotenv import load_dotenv

# General Helper Functions

def ms_to_minutes_seconds(duration_ms):
    """
    This is a helper function that converts duration of songs
    in milliseconds to minutes and seconds.

    Original author: David Mombourquette

    Parameters
    ----------
    duration_ms : float
        the duration of a song in ms

    Returns
    -------
    duration_formatted : string
        a string representation of the duration
        of a song in minutes and seconds
    """
    minutes = duration_ms // 60000
    seconds = (duration_ms % 60000) // 1000
    return f"{minutes} min {seconds} sec"

def convert_to_datetime(df, column):
    """
    This is a helper function that converts a date column with
    mixed values into a datetime function.

    Original Author: Katy Mombourquette

    Parameters
    ----------
    df : pandas.core.frame.DataFrame
        the dataframe containing the date column

    column: string
        the date column to be converted

    Returns
    -------
    df : pandas.core.frame.DataFrame
        the dataframe with the converted
        date column
    """

    df[column] = df[column].astype(str)

    # fill date column with a placeholder
    # this avoids errors when string matching
    df[column] = df[column].fillna('NA')

    # string match different cases of values

    # case one: 'YYYY'
    year_only_mask = df[column].str.match(r'^\d{4}$')

    # case two: 'YYYY-MM'
    year_month_mask = df[column].str.match(r'^\d{4}-\d{2}$')

    # convert identified rows to datetime with default month and day
    # set errors to NaT with errors='coerce'
    df.loc[year_only_mask, column] = pd.to_datetime(df.loc[year_only_mask, column].astype(str) + '-01-01', errors='coerce')
    df.loc[year_month_mask, column] = pd.to_datetime(df.loc[year_month_mask, column] + '-01', errors='coerce')

    # convert the other rows
    df.loc[~(year_only_mask | year_month_mask), column] = pd.to_datetime(df.loc[~(year_only_mask | year_month_mask), column], errors='coerce')

    # convert any remaining string values
    df[column] = pd.to_datetime(df[column], errors='coerce')

    # replace placeholder with NaN
    df[column] = df[column].replace('NA', np.nan)

    return df

def rate_artist_complexity(df):
    """
    Counts artist genres to rate artist complexity
    """
    # convert genres to a list type so they can be counted
    df["genres"] = df["genres"].apply(ast.literal_eval)

    # count genres
    df["genre_count"] = df["genres"].apply(len)

    return df

# Helper Functions for get_spotify_data()

load_dotenv()
default_id = os.getenv("CLIENT_ID")
default_secret = os.getenv("CLIENT_SECRET")

def spotify_access(client_id_param=default_id, client_secret_param=default_secret):
    """
    Returns the access token used to make requests from the Spotify API
    Requires the user to request a client id and secret from Spotify's API
    webpage.

    Original author: David Mombourquette

    Parameters
    ----------
    client_id_param: string
        the user's client id

    client_secret_param: string
        the user's client secret

    Returns
    -------
    access_token: string
        the access token used in API calls
    """
    # Set the necessary parameters
    token_url = 'https://accounts.spotify.com/api/token'
    payload = {'grant_type': 'client_credentials'}

    # Base64 encode the client_id and client_secret
    credentials = f"{client_id_param}:{client_secret_param}"
    base64_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    headers = {'Authorization': f'Basic {base64_credentials}'}

    # Use 'requests' Session for improved performance
    with requests.Session() as session:
        # Send the POST request to obtain the access token
        response = session.post(token_url, data=payload, headers=headers)

    # Check if the request was successful
    response.raise_for_status()

    # Extract the access token from the response
    access_token = response.json().get('access_token')

    return access_token

def read_spotify_json(file_list):
    print(f"Reading Spotify JSON files for.")

    audio_data = []

    # Loop through each uploaded file
    for file in file_list:
        if file.name.startswith("Streaming_History_Audio") and file.name.endswith(".json"):
            # Read directly as a pandas DataFrame
            try:
                data = pd.read_json(file)
                audio_data.append(data)
            except ValueError:
                # Fallback if read_json fails (e.g. non-UTF8 or unexpected structure)
                file.seek(0)
                data = json.load(file)
                audio_data.append(pd.DataFrame(data))

    if not audio_data:
        raise FileNotFoundError("No valid Spotify audio history JSON files found.")

    stats_raw = pd.concat(audio_data, ignore_index=True)
    print(f"Loaded {len(stats_raw)} records from Spotify JSON files.")
    return stats_raw


def get_user_track_ids(user_data):
    """
    Returns a list of the unique track ids
    in a user's Spotify listening data.

    Track ids are sorted by most-least listened to
    songs in the dataset

    Original Author: Katy Mombourquette

    Parameters
    ----------
    user_data : pandas.core.frame.DataFrame
        a dataframe of clean user
        Spotify listening data

    Returns
    -------
    track_ids : list
        a list of unique track ids
        found in the user's Spotify listening data
    """
    # create track id column
    user_data['track_id'] = user_data['spotify_track_uri'].str.split(':').str[-1]

    # count tracks
    track_counts = user_data['track_id'].value_counts().reset_index()
    track_counts.columns = ['track_id', 'count']

    # sort track_ids by count
    track_counts = track_counts.sort_values(by='count', ascending=False)

    # create a list of track_ids
    track_ids = track_counts['track_id'].tolist()

    return track_ids

from tqdm import tqdm

def get_multiple_tracks_response(track_ids, access_token):
    """
    Fetches metadata for multiple track IDs from Spotify API using Streamlit-friendly progress bar.
    """
    basic_df = pd.DataFrame()

    chunk_size = 50
    total = len(track_ids)
    num_chunks = (total + chunk_size - 1) // chunk_size

    # Streamlit progress UI
    st.write("🎵 Fetching track metadata from Spotify...")
    progress_bar = st.progress(0)
    status_text = st.empty()

    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min(start_idx + chunk_size, total)
        chunk = track_ids[start_idx:end_idx]
        chunk_str = ",".join(chunk)

        api_url = 'https://api.spotify.com/v1/tracks'
        headers = {
            'Authorization': f'Bearer {access_token}'
        }
        params = {
            'ids': chunk_str
        }

        response = requests.get(api_url, headers=headers, params=params)

        try:
            response_data = response.json()
        except ValueError:
            st.warning(f"⚠️ Failed to decode JSON response for chunk {i+1}.")
            st.text(f"Status code: {response.status_code}")
            st.text(f"Response text:\n{response.text[:500]}")
            continue

        # Iterate through each track in the response
        for track_data in response_data.get('tracks', []):
            track_response = {
                'name': track_data.get('name', 'No Name'),
                'artist': track_data['album']['artists'][0]['name'],
                'album_name': track_data['album']['name'],
                'track_number': track_data.get('track_number', 0),
                'artist_id': track_data['artists'][0]['id'],
                'album_date': track_data['album']['release_date'],
                'album_track_count': track_data['album']['total_tracks'],
                'track_id': track_data['id'],
                'popularity': track_data['popularity']
            }

            track_df = pd.DataFrame([track_response])
            basic_df = pd.concat([basic_df, track_df])

        # Update Streamlit UI
        percent_complete = (i + 1) / num_chunks
        progress_bar.progress(percent_complete)
        status_text.text(f"Processed tracks {start_idx + 1}–{end_idx} of {total}")

    # Final UI update
    progress_bar.empty()
    status_text.success("✅ Done fetching tracks!")

    basic_df.set_index('track_id', inplace=True)
    return basic_df


def get_multiple_features_response(track_ids, access_token):
    """
    Gathers the features information on the track_ids from the Spotify API.

    Parameters
    ----------
    track_ids : list
        List of track IDs to fetch audio features for.
    access_token : string
        Access token for Spotify API authentication.

    Returns
    -------
    feature_df : pandas.DataFrame
        DataFrame containing audio features of the tracks.
    """
    chunk_size = 50
    feature_df = pd.DataFrame()

    for i in range(0, len(track_ids), chunk_size):
        chunk = track_ids[i:i + chunk_size]
        track_ids_str = ",".join(chunk)

        api_url = f'https://api.spotify.com/v1/audio-features?ids={track_ids_str}'
        headers = {'Authorization': f'Bearer {access_token}'}

        response = requests.get(api_url, headers=headers)

        # Check if response is valid
        if response.status_code != 200:
            print(f"Error fetching audio features: {response.json()}")
            continue

        response_data = response.json()

        # Handle cases where 'audio_features' key is missing
        if 'audio_features' not in response_data:
            print("Warning: 'audio_features' missing in API response.")
            continue

        for trk in response_data['audio_features']:
            # Skip if a feature is None
            if trk is None:
                continue

            df = pd.DataFrame.from_dict([trk])
            feature_df = pd.concat([feature_df, df], axis=0)

    if feature_df.empty:
        print("Warning: No audio features could be retrieved.")
    else:
        # Apply the custom function to format the 'duration_ms' column
        feature_df['duration_formatted'] = feature_df['duration_ms'].apply(ms_to_minutes_seconds)

        # Set the index of the DataFrame to track_id
        feature_df.set_index('id', inplace=True)

    return feature_df

def get_artist_ids(metadata):
    """
    Returns a list of the unique artist ids
    in a user's Spotify metadata

    Original Author: Katy Mombourquette

    Parameters
    ----------
    metadata : pandas.core.frame.DataFrame
        a dataframe of user
        Spotify metadata

    Returns
    -------
    artist_ids : list
        a list of unique artist ids
        found in the user's Spotify metadata
    """

    artist_ids = metadata['artist_id'].unique().tolist()
    return artist_ids

def get_multiple_artist_genres(metadata, access_token):
    """
    Adds genre information to a user's Spotify metadata

    Original Author: Katy Mombourquette, based on functions by
    David Mombourquette

    Parameters
    ----------
    metadata : pandas.core.frame.DataFrame
        a dataframe of clean user
        Spotify metadata

    access_token: string
        a string representing the access token needed
        to access Spotify's API

    Returns
    -------
    basic_df : pandas.core.frame.DataFrame
        a dataframe consisting of Spotify
        metadata combined with genre information
    """

    artist_ids = get_artist_ids(metadata)

    basic_df = pd.DataFrame()

    # Process the ids in chunks
    chunk_size = 50
    for i in range(0, len(artist_ids), chunk_size):
        chunk = artist_ids[i:i + chunk_size]

        # Convert chunk to comma-separated string
        chunk_str = ",".join(chunk)

        # Set the necessary parameters for Spotify API
        api_url = 'https://api.spotify.com/v1/artists'
        headers = {
            'Authorization': f'Bearer {access_token}'
        }
        params = {
            'ids': chunk_str
        }

        # Send the GET request to retrieve artist information for multiple artists
        response = requests.get(api_url, headers=headers, params=params)
        response_data = response.json()

        if 'error' in response_data:
            print(f"Error: {response_data['error']['message']}")
            # Skip this chunk and continue with the next one
            continue

        # Initialize lists to store artist IDs and genres
        ids = []
        genres = []

        # Iterate over each artist in the response data
        for artist in response_data['artists']:
            ids.append(artist['id'])
            genres.append(artist['genres'])

        # Create a dictionary for the response
        genre_response = {
            'artist_id': ids,
            'genres': genres
        }

        # Convert the dictionary to a DataFrame
        genre_df = pd.DataFrame(genre_response)

        # Append the DataFrame to basic_df
        basic_df = pd.concat([basic_df, genre_df])

    return basic_df

def get_metadata(track_ids, access_token):
    """
    Takes a list of unique track ids and retrieves
    metadata for each track in df form

    Original Author: Katy Mombourquette

    Parameters
    ----------
    track_ids : list
        a list of unique track ids
        found in the user's Spotify listening data

    access_token: string
        a string representing the access token needed
        to access Spotify's API

    Returns
    -------
    metadata : pandas.core.frame.DataFrame
        a df containing metadata information
        about each track in track_ids
    """

    # get track & feature information
    track_responses = get_multiple_tracks_response(track_ids, access_token)

    # the audio-features endpoint was deprecated in Nov 2024 :-(
    # features_responses = get_multiple_features_response(track_ids, access_token)

    # handle duplicate indices
    # if features_responses.index.is_unique == False:
        # features_responses = features_responses[~features_responses.index.duplicated(keep='first')]

    # concatenate track & feature information
    # metadata = pd.concat([track_responses.reset_index(drop=True), features_responses.reset_index(drop=True)], axis=1)

    metadata = track_responses.copy()

    # add in genre information
    genres_responses = get_multiple_artist_genres(track_responses, access_token)
    metadata = pd.merge(metadata, genres_responses, on='artist_id', how='outer')

    return metadata

def get_spotify_metadata(df, client_id=default_id, client_secret=default_secret):
    """
    Requests metadata from Spotify's Web Development APIs based on
    track ids found in user's Spotify listening history.

    Original Author: Katy Mombourquette

    Parameters
    ----------

    client_id: string
        a string representation of the user's API client id

    client_secret: string
        a string representation of the user's API client secret

    Returns
    -------
    metadata : pandas.core.frame.DataFrame
        A dataframe containing the user's Spotify
        metadata
    """

    # read user's spotify json files
 
    raw_data = df
    print("Raw data loaded successfully.")

    # get unique track ids from user data
    track_ids = get_user_track_ids(raw_data)
    print(f"Extracted {len(track_ids)} unique track IDs.")

    # get access token for api
    access_token = spotify_access(client_id, client_secret)
    print("Access token retrieved successfully.")

    # get metadata for each track id
    metadata = get_metadata(track_ids, access_token)
    print("Metadata fetched successfully.")

    # return final df
    return metadata


def combine_raw_meta(df, client_id=default_id, client_secret=default_secret):
    """
    Combines the user's metadata with their raw Spotify
    listening data

    Original Author: Katy Mombourquette

    Parameters
    ----------
    client_id: string
        a string representation of the user's API client id

    client_secret: string
        a string representation of the user's API client secret

    Returns
    -------
    user_listening : pandas.core.frame.DataFrame
        A dataframe containing the user's combined
        Spotify metadata and raw listening data
    """

    # read in the user's raw stats
    stats_raw = df

    # request metadata information from Spotify API
    metadata = get_spotify_metadata(df, client_id, client_secret)

    # create track id column in one df
    stats_raw['track_id'] = stats_raw['spotify_track_uri'].apply(lambda x: x.split(":")[2] if x is not None else None)

    # create track-artist columns to merge on
    stats_raw['track-artist'] = stats_raw['master_metadata_track_name'].str.lower() + ' - ' + stats_raw['master_metadata_album_artist_name'].str.lower()
    metadata['track-artist'] = metadata['name'].str.lower() + ' - ' + metadata['artist'].str.lower()

    # drop duplicates
    stats_raw = stats_raw.drop_duplicates()
    metadata = metadata.drop_duplicates(subset=['track-artist'])

    # Merge the dataframes on the track-artist column
    user_listening = pd.merge(stats_raw, metadata, on='track-artist', how='left') # only keep rows in the user's listening stats

    # simple cleaning
    if '0' in user_listening.columns:
        user_listening = user_listening.drop(columns=['0'])
    if 0 in user_listening.columns:
        user_listening = user_listening.drop(columns=[0])

    return user_listening


def clean_listening_data(user_listening):
    """
    Cleans the user's combined meta and raw
    Spotify data.

    Original Author: Katy Mombourquette

    Parameters
    ----------

    user_listening: pandas.core.frame.DataFrame
        A dataframe containing the user's combined
        meta and raw Spotify data

    Returns
    -------
    user_listening : pandas.core.frame.DataFrame
        A dataframe containing the user's cleaned Spotify
        metadata and raw data
    """

    # rename columns
    user_listening = user_listening.rename(columns={
                                                'ts': 'timestamp_listened',
                                                'ms_played': 'ms_listened',
                                                #'duration_ms': 'song_duration_ms',
                                                #'duration_formatted': 'song_duration_formatted',
                                                'name':'track',
                                                'offline_timestamp':'offline_timestamp_listened',
                                                'album_name': 'album',
                                                'album_date': 'album_release_date',
                                                'ip_addr_decrypted': 'ip_addr'
                                                })

    # drop irrelevant columns
    irrelevant_columns = ['master_metadata_track_name',
                        'master_metadata_album_artist_name',
                        'master_metadata_album_album_name',
                        'spotify_track_uri',
                        'episode_name',
                        'episode_show_name',
                        'spotify_episode_uri',
                        'skipped',
                        'track_href',
                        'analysis_url',
                        'user_agent_decrypted',
                        'type',
                        'uri']

    user_listening = user_listening.drop([col for col in irrelevant_columns if col in user_listening.columns], axis=1)

    # convert date columns
    user_listening['timestamp_listened'] = pd.to_datetime(user_listening['timestamp_listened'])
    user_listening['offline_timestamp_listened'] = pd.to_datetime(user_listening['offline_timestamp_listened'])
    user_listening = convert_to_datetime(user_listening, 'album_release_date')

    # create album-artist column
    user_listening['album-artist'] = user_listening['album'].str.lower() + " - " + user_listening['artist'].str.lower()

    # drop nulls in track column
    user_listening = user_listening.dropna(subset=['track'])

    # cast column types
    user_listening['track_number'] = user_listening['track_number'].astype('int')
    user_listening['album_track_count'] = user_listening['album_track_count'].astype('int')
    # user_listening.loc[user_listening['time_signature'].notnull(), 'time_signature'] = user_listening.loc[user_listening['time_signature'].notnull(), 'time_signature'].astype('int')
    #user_listening['username'] = user_listening['username'].astype('str')
    # user_listening['time_signature'] = user_listening['time_signature'].apply(lambda x: int(x) if pd.notnull(x) else x)

    # get genre_counts
    #user_listening = rate_artist_complexity(user_listening)


    # reorder columns

    # old, when audio-features was available
    # user_listening = user_listening[['username', 'track', 'track_id', 'artist', 'artist_id', 'track-artist',
    #                                 'album', 'album-artist', 'album_release_date', 'album_track_count', 'track_number', 'song_duration_ms',
    #                                 'song_duration_formatted', 'timestamp_listened', 'platform', 'conn_country', 'ip_addr',
    #                                 'ms_listened', 'reason_start', 'reason_end', 'shuffle', 'offline', 'offline_timestamp_listened',
    #                                 'incognito_mode', 'popularity', 'danceability', 'energy', 'loudness', 'speechiness', 'acousticness',
    #                                 'instrumentalness', 'liveness', 'valence', 'key', 'mode', 'tempo', 'time_signature', 'genres']]


    user_listening = user_listening[['track', 'track_id', 'artist', 'artist_id', 'track-artist',
                                    'album', 'album-artist', 'album_release_date', 'album_track_count', 'track_number', 'timestamp_listened', 'platform', 'conn_country', 'ip_addr',
                                    'ms_listened', 'reason_start', 'reason_end', 'shuffle', 'offline', 'offline_timestamp_listened',
                                    'incognito_mode', 'popularity', 'genres']]

    return user_listening

def first_year_listened(df):
    # Extract year from timestamp_listened
    df['timestamp_listened'] = pd.to_datetime(df['timestamp_listened'])
    print(df['timestamp_listened'].dtype)

    df["Year_Listened"] = df['timestamp_listened'].dt.year

    # Group by track-artist & aggregate unique years
    years_listened = df.groupby('track-artist')['Year_Listened'].agg(list).reset_index()
    years_listened['Year_Listened'] = years_listened['Year_Listened'].apply(lambda x: list(set(x)))

    # Create a new column with the first year listened
    years_listened["first_year_listened"] = years_listened["Year_Listened"].apply(lambda x: min(x))

    # Merge years_listened back into df
    df = pd.merge(df, years_listened[['track-artist', 'first_year_listened']], on='track-artist', how='left')

    # Repeat for artists
    artists_first_listen = df.groupby('artist')['Year_Listened'].agg(list).reset_index()
    artists_first_listen['Year_Listened'] = artists_first_listen['Year_Listened'].apply(lambda x: list(set(x)))
    artists_first_listen["first_year_artist_listened"] = artists_first_listen["Year_Listened"].apply(lambda x: min(x))

    # Merge artists_first_listen back into df
    df = pd.merge(df, artists_first_listen[['artist', 'first_year_artist_listened']], on='artist', how='left')

    # Drop unnecessary columns
    df.drop(columns=['Year_Listened'], inplace=True)

    return df

def get_spotify_data(df, client_id=default_id, client_secret=default_secret):
    """
    Returns an expanded and cleaned version of a user's Spotify json streaming history.

    The function scans the working directory for json files beginning with
    "Streaming_History_Audio", requests metadata information from the Spotify API,
    and combines it with the raw data.

    Original Author: Katy Mombourquette

    Parameters
    ----------
    client_id: string
        a string representation of the user's API client id

    client_secret: string
        a string representation of the user's API client secret

    Returns
    -------
    df : pandas.core.frame.DataFrame
        A dataframe containing the user's cleaned Spotify
        metadata and raw data
    """
    print(f"Starting `get_spotify_data`")
    data = df
    print("Combined raw data and metadata.")
    data = clean_listening_data(data)
    print("Cleaned listening data.")
    data = first_year_listened(data)
    print("Added first year listened columns.")

    return data

# More functions for cleaning purposes

def get_general_genre(specific_genre, list_of_genres=['k-pop', 'pop', 'rock', 'classical', 'hip hop',
                                                    'country', 'indie','electronic', 'jazz', 'punk',
                                                    'funk', 'folk', 'alternative']):

    """
    Returns the general genre for a given specific genre

    Original Author: Katy Mombourquette

    Parameters
    ----------
    specific_genre: string
        a string representation of a genre

    list_of_genres: list of strings
        optional argument - a list of general genres to extract
        from the specific spotify-assigned genres.
        General genres should be key words that exist in the
        specific genres.

    Original Author: Katy Mombourquette

    Returns
    -------
    genre : string
        A a string representation of a general
        genre that best describes the specific genre
    """

    specific_genre_lower = specific_genre.lower()

    for genre in list_of_genres:
        if specific_genre == 'unknown genre':
            return 'unknown genre'
        elif specific_genre == 'folk-pop':
            return 'folk'
        elif genre in specific_genre_lower:
            return genre

    return 'unique genre'


def convert_key_names(df):
    """
    Returns the given name for numeric representations
    of musical keys

    Original Author: Katy Mombourquette

    Parameters
    ----------
    df: pandas.core.frame.DataFrame
        a dataframe including a column with
        numerical keys

    Returns
    -------
    df: pandas.core.frame.DataFrame
        a dataframe including a column with
        the converted names of numerical keys
    """

    df['key_name'] = df['key']

    key_map = {
        0:'C',
        1:'C#/Db',
        2:'D',
        3:'D#/Eb',
        4:'E',
        5:'F',
        6:'F#/Gb',
        7:'G',
        8:'G#/Ab',
        9:'A',
        10:'A#/Bb',
        11: 'B'
    }

    df = df.replace({"key_name": key_map})

    return df

def convert_mode_names(df):
    """
    Returns the given name for numeric representations
    of musical modes

    Original Author: Katy Mombourquette

    Parameters
    ----------
    df: pandas.core.frame.DataFrame
        a dataframe including a column with
        numerical modes

    Returns
    -------
    df: pandas.core.frame.DataFrame
        a dataframe including a column with
        the converted names of numerical modes
    """
    df['mode_name'] = df['mode']

    mode_map = {
        0:'Minor',
        1: 'Major'
    }

    df = df.replace({"mode_name": mode_map})

    return df

def clean_spdata_for_analysis(df, list_of_genres=['k-pop', 'pop', 'rock', 'classical', 'hip hop',
                                                    'country', 'indie','electronic', 'jazz', 'punk',
                                                    'funk', 'folk', 'alternative']):
    """
    Cleans spotify data for the purpose of analysis.

    Original Author: Katy Mombourquette

    Parameters
    ----------
    df: pandas.core.frame.DataFrame
        a dataframe spotify data information, ideally
        from the get_spotify_data() function

    list_of_genres: list of strings
        optional argument - a list of general genres to extract
        from the specific spotify-assigned genres.
        General genres should be key words that exist in the
        specific genres.

    Returns
    -------
    df: pandas.core.frame.DataFrame
        a dataframe of cleaned data for analysis
    """

    import uuid

    
    # create unique id
    df['unique_id'] = [uuid.uuid4() for _ in range(len(df))]

    # create individual genre columns

    df['genres'] = df['genres'].astype(str)

    # Determine the maximum number of genres present in any row
    max_genres = df['genres'].str.count(',') + 1
    max_genres = max_genres.max()

    # split the 'genres' column into multiple columns
    df_genres = df['genres'].str.strip("[]").str.split(", ", expand=True)

    # rename the columns to genre1, genre2, ...
    df_genres.columns = [f'genre{i+1}' for i in range(max_genres)]

    # concatenate the original DataFrame and the new genres DataFrame
    df = pd.concat([df, df_genres], axis=1)

    # strip quotes from genres
    for column in [f'genre{i+1}' for i in range(max_genres)]:
        df[column] = df[column].str.strip("'")

    # assign 'unknown genre' to empty genres
    df['genre1'] = df['genre1'].apply(lambda x: 'unknown genre' if x == '' else x)

    # create general genre cols
    df['general_genre'] = df['genre1'].apply(lambda specific_genre: get_general_genre(specific_genre, list_of_genres))

    # re-cast album datetime types
    df = convert_to_datetime(df, 'album_release_date')

    # convert numerical keys and modes to their names
    #df = convert_key_names(df)
    #df = convert_mode_names(df)

    # create categorical versions of feature measures (danceability, etc.)

    # def category_conversion(x, column, q1, q3):
    #     if x <= q1:
    #         x = f'Low {column}'
    #     elif x >= q3:
    #         x = f'High {column}'
    #     else:
    #         x = f'Medium {column}'
    #     return x

    # feat_cols = ['danceability', 'energy', 'loudness', 'speechiness',
    #                 'acousticness', 'instrumentalness', 'liveness', 'valence']

    # # apply category_conversion to feature columns
    # for column in feat_cols:
    #     q1 = df[column].quantile(0.25)
    #     q3 = df[column].quantile(0.75)
    #     df[f'{column}_category'] = df[f'{column}'].apply(category_conversion, column=f'{column}'.capitalize(), q1=q1, q3=q3)

    # # reorder columns


    # old version, several of these are no longer valid after audio-features was deprecated
    # df = df[[

    # 'unique_id', 'username','track','track_id','artist','artist_id','track-artist','album','album-artist','album_release_date',
    # 'album_track_count','track_number','song_duration_ms','song_duration_formatted','timestamp_listened','platform',
    # 'conn_country','ip_addr_decrypted','ms_listened','reason_start','reason_end','shuffle','offline',
    # 'offline_timestamp_listened','incognito_mode','popularity','danceability', 'danceability_category','energy','energy_category','loudness','loudness_category',
    # 'speechiness','speechiness_category','acousticness','acousticness_category','instrumentalness','instrumentalness_category','liveness','liveness_category','valence',
    # 'valence_category','key','key_name','mode','mode_name','tempo','time_signature','genres','genre1','genre2','genre3','genre4','genre5','genre6','genre7','genre8',
    # 'genre9','genre10','genre11','general_genre'

    #     ]]

    # # rename ip addr for new file
    # df = df.rename(columns={'ip_addr_decrypted': 'ip_addr'})

    desired_cols = [

    'unique_id','track','track_id','artist','artist_id','track-artist','album','album-artist','album_release_date',
    'album_track_count','track_number','timestamp_listened','platform',
    'conn_country','ip_addr','ms_listened','reason_start','reason_end','shuffle','offline',
    'offline_timestamp_listened','incognito_mode','popularity','genres','genre1','genre2','genre3','genre4','genre5','genre6','genre7','genre8',
    'genre9','genre10','genre11','general_genre', 'first_year_listened', 'first_year_artist_listened'

        ]

    columns = [col for col in desired_cols if col in df.columns]

    df = df[columns]

    return df


def pivot_features(df, id_vars =  ['unique_id','timestamp_listened',
                                    'key','key_name','mode','mode_name',
                                    'genre1','general_genre','artist'],
                        value_vars = ['danceability', 'energy','loudness','speechiness',
                                        'acousticness','instrumentalness', 'liveness', 'valence']):
    """
    Creates a pivot table from the output df of clean_spdata_for_analysis()
    So changes in features can be measured over time

    Original Author: Katy Mombourquette

    Parameters
    ----------
    df: pandas.core.frame.DataFrame
        a dataframe spotify data information, ideally
        from the clean_spdata_for_analysis() function

    id_vars: list of strings
        the columns to include in the pivot table besides the features.
        a default set are included but the use may enter their own

    value_vars: list of strings
        the columns to include as values (the features).
        a default set are included but the user may enter their own

    Returns
    -------
    features_pivot: pandas.core.frame.DataFrame
        a pivoted dataframe
    """

    features_pivot = pd.melt(df, id_vars=id_vars, value_vars=value_vars, var_name='Feature', value_name='Measure')

    return features_pivot

# get_lyrics functions

def get_lyrics_access_token(client_id, client_secret):
    # Endpoint to obtain access token
    token_url = 'https://api.genius.com/oauth/token'

    # Payload data for authentication
    payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }

    # Send POST request to obtain access token
    response = requests.post(token_url, data=payload)

    if response.status_code == 200:
        # Parse JSON response and extract access token
        access_token = response.json()['access_token']
        return access_token
    else:
        # Handle error response
        print("Error fetching access token:", response.text)
        return None
    
# clean lyrics

# Function to remove "see [artist]" pattern using artist names
def remove_artist_reference(row):
    artist = row['artist']
    lyrics = row['lyrics']
    return lyrics.replace(f"see {artist}", '')

def clean_lyrics(lyrics, df_2):

    # Remove any number followed by "Embed" from the end of the string
    lyrics = re.sub(r'\d+Embed$', '', lyrics)

    # Remove anything between square brackets and the brackets themselves
    lyrics = re.sub(r'\[.*?\]', '', lyrics)
    
    # Remove any new line markers '\n'
    lyrics = lyrics.replace('\n', ' ')

    # Remove text from the beginning until the end of the word "Lyrics"
    lyrics = re.sub(r'^.*?Lyrics', '', lyrics)
    
    # Remove XYyou
    lyrics = re.sub(r'\b\d+\w+', '', lyrics)
    
    # Remove any leading or trailing whitespace
    lyrics = lyrics.strip()

    # Assuming df_2 is your DataFrame
    # Extract the artist names from the 'track-artist' column
    df_2['artist'] = df_2['track-artist'].str.split(' - ').str[-1].str.strip()


    # Apply the function to remove artist references
    df_2['lyrics'] = df_2.apply(remove_artist_reference, axis=1)

    # Drop the 'artist' column if it's no longer needed
    df_2.drop(columns=['artist'], inplace=True)
    
    return lyrics

# ----- Extra Table creation for PBIX analysis -----

def get_yearly_song_counts(df):

    df['year_listened'] = df['timestamp_listened'].dt.year

    df = df.groupby(['year_listened','track-artist'])['track_id'].count().reset_index()
    
    return df

def get_similar_songs(data):

    data = data.dropna(subset=['track-artist', 'danceability', 'tempo', 'general_genre', 'acousticness', 'energy', 'liveness', 'instrumentalness'])

    # Group the song features by track-artist and calculate the mean for each feature
    song_features = data.groupby('track-artist').agg({
        'danceability': 'mean',
        'tempo': 'mean',
        'general_genre': 'first',
        'acousticness': 'mean',
        'energy': 'mean',
        'liveness': 'mean',
        'instrumentalness': 'mean'
    }).reset_index()

    # Handle NaN values by dropping rows with any NaN values
    song_features = song_features.dropna()

    # One-hot encode the general_genre column
    encoder = OneHotEncoder(sparse_output=False)
    general_genre_encoded = encoder.fit_transform(song_features[['general_genre']])

    # Convert the encoded general_genre into a DataFrame
    general_genre_columns = encoder.get_feature_names_out(['general_genre'])
    general_genre_df = pd.DataFrame(general_genre_encoded, columns=general_genre_columns)

    # Select the numerical features and concatenate with one-hot encoded general_genre
    X = pd.concat([song_features[['danceability', 'tempo', 'acousticness', 'energy', 'liveness', 'instrumentalness']], general_genre_df], axis=1)

    # Drop rows with NaN values from X
    X = X.dropna()

    # Initialize a list to store results
    results = []

    # Iterate through each song
    for index, row in song_features.iterrows():
        song_name = row['track-artist']
        
        # Extract the feature values of the given song
        given_song_numerical = row[['danceability', 'tempo', 'acousticness', 'energy', 'liveness', 'instrumentalness']]
        given_song_genre_encoded = encoder.transform([row[['general_genre']]])
        
        # Concatenate the numerical and one-hot encoded features
        given_song_features = pd.concat([
            pd.DataFrame([given_song_numerical.values], columns=['danceability', 'tempo', 'acousticness', 'energy', 'liveness', 'instrumentalness']),
            pd.DataFrame(given_song_genre_encoded, columns=general_genre_columns)
        ], axis=1).values

        # Ensure there are no NaNs in the given song features
        if pd.isna(given_song_features).any():
            similar_songs = []
        else:
            # Calculate the Euclidean distances between the given song and all other songs
            distances = euclidean_distances(X, given_song_features)

            # Add the distances to the song_features DataFrame
            song_features['distance_to_given_song'] = distances

            # Sort by the closest distance and exclude the given song itself
            most_similar_songs = song_features[song_features['track-artist'] != song_name].sort_values(
                by='distance_to_given_song', ascending=True
            )

            # Get the top 10 most similar songs
            top_10_similar_songs = most_similar_songs[['track-artist']].head(10)
            
            # Convert to a single string
            similar_songs_str = ', '.join(top_10_similar_songs['track-artist'].values)
        
        # Append the result
        results.append({'track-artist': song_name, 'most_similar_songs': similar_songs_str})

    # Create a DataFrame from the results
    results_df = pd.DataFrame(results)

    # Display the results DataFrame
    return results_df

def get_clean_data(person, client_id=default_id, client_secret=default_secret, output_dir="analysis_files/"):
    """
    High-level function to generate all analysis files at once.
    Keeps individual functions modular while automating the workflow.
    
    Parameters:
    - client_id: Spotify API client ID (default is prefilled for testing)
    - client_secret: Spotify API client secret (default is prefilled for testing)
    - output_dir: Directory to save output files, relative to the script location.
    
    Outputs CSV files for cleaned data, yearly song counts, pivoted features, etc.
    """
    
    # Main cleaned dataset
    cleaned_data = get_spotify_data(person, client_id, client_secret)
    
    try:
        cleaned_analysis_data = clean_spdata_for_analysis(cleaned_data)
    except Exception as e:
        print(f"issue cleaning data for analysis: {e}")

    return cleaned_analysis_data

