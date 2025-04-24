import uuid
import pandas as pd
import numpy as np

def get_dim_user(df, name):
    import uuid
    import pandas as pd

    # Load the existing dim_user file
    try:
        last_dim_user = pd.read_csv(f'analysis_files/{name}/tables/dim_user.csv')
        print('returned previous dim_user file')
    except FileNotFoundError:
        # If the file doesn't exist, initialize an empty DataFrame
        last_dim_user = pd.DataFrame(columns=['user_id', 'username', 'date_added'])

    df['username'] = name

    # Ensure we only proceed if df is not empty
    if df.empty:
        print("Input DataFrame is empty. Returning existing dim_user.")
        return last_dim_user

    # Extract unique usernames from the new DataFrame
    new_users = df[['username']].drop_duplicates()

    # Find genuinely new users (not already in last_dim_user)
    existing_usernames = set(last_dim_user['username'])
    new_users = new_users[~new_users['username'].isin(existing_usernames)]

    # Add relevant columns for new users
    if not new_users.empty:
        new_users['user_id'] = [str(uuid.uuid4()) for _ in range(len(new_users))]
        new_users['date_added'] = pd.Timestamp.now()

        # Reorder columns
        new_users = new_users[['user_id', 'username', 'date_added']]

        # Combine existing and new users into a single DataFrame
        updated_dim_user = pd.concat([last_dim_user, new_users], ignore_index=True)

    else:
        print("No new users to add.")
        updated_dim_user = last_dim_user

    # drop duplicates

    updated_dim_user.drop_duplicates(subset=['user_id'], inplace=True)

    # Save the updated dim_user to the file
    #updated_dim_user.to_csv('dim_user.csv', index=False)
    print (updated_dim_user.columns)

    return updated_dim_user

def get_dim_track(df,name):
    # List of desired columns
    columns = [
    'track_id', 'track', 'track-artist', 'track_number', 'song_duration_ms', 
    'popularity', 'danceability', 'danceability_category', 'energy', 
    'energy_category', 'loudness', 'loudness_category', 'speechiness', 
    'speechiness_category', 'acousticness', 'acousticness_category', 
    'instrumentalness', 'instrumentalness_category', 'liveness', 
    'liveness_category', 'valence', 'valence_category', 'key', 'key_name', 
    'mode', 'mode_name', 'tempo', 'time_signature', 'first_year_listened', 'date_added'
]

    # Load the existing dim_track file if it exists
    try:
        last_dim_track = pd.read_csv(f'analysis_files/{name}/tables/dim_track.csv')
        print('returned previous dim_track file')
    except FileNotFoundError:
        print("No dim_track file, generating empty dataframe.")
        # Initialize an empty DataFrame if the file doesn't exist
        last_dim_track = pd.DataFrame(columns=columns)

    # Ensure all columns exist in the new DataFrame, adding missing ones with NaN
    for col in columns:
        if col not in df.columns:
            df[col] = np.nan

    # Select and de-duplicate the desired columns in the new DataFrame
    new_dim_track = df[[col for col in columns if col != 'date_added']].drop_duplicates()

    # Find genuinely new tracks (not already in last_dim_track)
    existing_track_ids = set(last_dim_track['track_id'])
    new_dim_track = new_dim_track[~new_dim_track['track_id'].isin(existing_track_ids)]

    # Add date_added for new artists
    new_dim_track['date_added'] = pd.Timestamp.now()

    # strip whitespace
    last_dim_track.columns = last_dim_track.columns.str.strip()
    new_dim_track.columns = new_dim_track.columns.str.strip()

    # Combine existing and new tracks into a single DataFrame
    updated_dim_track = pd.concat([last_dim_track, new_dim_track], ignore_index=True)

    updated_dim_track.drop_duplicates(subset=['track_id'], inplace=True)

    # Save the updated dim_track to the file
    #updated_dim_track.to_csv('dim_track.csv', index=False)

    return updated_dim_track

def get_dim_artist(df, name):
    columns = [
        'artist_id', 'artist', 'first_year_artist_listened', 'genres', 'genre1', 'genre2',
        'genre3', 'genre4', 'genre5', 'genre6', 'genre7', 'genre8', 'genre9', 'genre10',
        'genre11', 'general_genre', 'date_added'
    ]

    # Load the existing dim_artist file if it exists
    try:
        last_dim_artist = pd.read_csv(f'analysis_files/{name}/tables/dim_artist.csv')
    except FileNotFoundError:
        last_dim_artist = pd.DataFrame(columns=columns)

    # Ensure all columns exist in the new DataFrame
    for col in columns:
        if col not in df.columns:
            df[col] = np.nan

    # Select and de-duplicate the desired columns in the new DataFrame
    new_dim_artist = df[[col for col in columns if col != 'date_added']].drop_duplicates()

    # Find genuinely new artists
    existing_artist_ids = set(last_dim_artist['artist_id'])
    new_dim_artist = new_dim_artist[~new_dim_artist['artist_id'].isin(existing_artist_ids)]

    # Add date_added for new artists
    new_dim_artist['date_added'] = pd.Timestamp.now()

    # Combine existing and new artists
    updated_dim_artist = pd.concat([last_dim_artist, new_dim_artist], ignore_index=True)

    updated_dim_artist.drop_duplicates(subset=['artist_id'], inplace=True)

    # Save the updated dim_artist to the file
    #updated_dim_artist.to_csv('dim_artist.csv', index=False)

    return updated_dim_artist

def get_dim_album(df, name):
    columns = ['album', 'album-artist', 'album_release_date', 'album_track_count', 'date_added']

    # Load the existing dim_album file if it exists
    try:
        last_dim_album = pd.read_csv(f'analysis_files/{name}/tables/dim_album.csv')
    except FileNotFoundError:
        last_dim_album = pd.DataFrame(columns=['album_id'] + columns)

    # Ensure all columns exist in the new DataFrame
    for col in columns:
        if col not in df.columns:
            df[col] = np.nan

    # Select and de-duplicate the desired columns in the new DataFrame
    new_dim_album = df[[col for col in columns if col != 'date_added']].drop_duplicates()

    # Find genuinely new albums
    existing_albums = set(last_dim_album['album'])
    new_dim_album = new_dim_album[~new_dim_album['album'].isin(existing_albums)]

    # Add album_id and date_added for new albums
    new_dim_album['album_id'] = [str(uuid.uuid4()) for _ in range(len(new_dim_album))]
    new_dim_album['date_added'] = pd.Timestamp.now()

    # Reorder columns
    new_dim_album = new_dim_album[['album_id'] + columns]

    # Combine existing and new albums
    updated_dim_album = pd.concat([last_dim_album, new_dim_album], ignore_index=True)

    updated_dim_album.drop_duplicates(subset=['album_id'], inplace=True)

    # Save the updated dim_album to the file
    # updated_dim_album.to_csv('dim_album.csv', index=False)

    return updated_dim_album

def get_dim_location(df,name):
    columns = ['conn_country', 'ip_addr', 'date_added']

    # Load the existing dim_location file if it exists
    try:
        last_dim_location = pd.read_csv(f'analysis_files/{name}/tables/dim_location.csv')
        if 'ip_addr_decrypted' in last_dim_location.columns:
            last_dim_location.rename(columns={'ip_addr_decrypted': 'ip_addr'}, inplace=True)
    except FileNotFoundError:
        last_dim_location = pd.DataFrame(columns=['location_id'] + columns)

    # Ensure all columns exist in the new DataFrame
    for col in columns:
        if col not in df.columns:
            df[col] = np.nan

    # Select and de-duplicate the desired columns in the new DataFrame
    new_dim_location = df[[col for col in columns if col != 'date_added']].drop_duplicates()

    # Find genuinely new locations
    existing_locations = set(last_dim_location['ip_addr'])
    new_dim_location = new_dim_location[~new_dim_location['ip_addr'].isin(existing_locations)]

    # Add location_id and date_added for new locations
    new_dim_location['location_id'] = [str(uuid.uuid4()) for _ in range(len(new_dim_location))]
    new_dim_location['date_added'] = pd.Timestamp.now()

    # Reorder columns
    new_dim_location = new_dim_location[['location_id'] + columns]

    # Combine existing and new locations
    updated_dim_location = pd.concat([last_dim_location, new_dim_location], ignore_index=True)

    updated_dim_location.drop_duplicates(subset=['location_id'], inplace=True)

    # Save the updated dim_location to the file
    #updated_dim_location.to_csv('dim_location.csv', index=False)

    return updated_dim_location

def get_fact_listening(df_new, name, dim_user, dim_album, dim_location):

    # Load existing fact_listening data
    try:
        # read the file
        df_old = pd.read_csv(f'analysis_files/{name}/tables/fact_listening.csv')

        # return only new data
        df_old['timestamp_listened'] = pd.to_datetime(df_old['timestamp_listened'])
        df_old['date'] = df_old['timestamp_listened'].dt.date 
        max_date_listened = df_old['date'].max()
        df_old = df_old[df_old['date'] >= max_date_listened]

    except FileNotFoundError:
        # Initialize an empty DataFrame if the file doesn't exist
        print("No fact_listening file found, generating new one")
        df_old = pd.DataFrame(columns=[
            'unique_id', 'user_id', 'track_id', 'artist_id', 'album_id', 'location_id', 
            'timestamp_listened', 'ms_listened', 'platform', 'reason_start', 'reason_end', 
            'shuffle', 'incognito_mode'
        ])

    # Concatenate new and old data
    df = pd.concat([df_new, df_old], ignore_index=True)

    # Drop any duplicate rows
    df.drop_duplicates()

    df['username'] = name

    # Merge with dim_user to add user_id
    df = df.merge(dim_user[['user_id', 'username']], how='left', on='username')
    df.drop(columns=['username', 'user_id_x'], inplace=True, errors='ignore')
    df.rename(columns={'user_id_y': 'user_id'}, inplace=True)



    # Drop unnecessary track-related columns
    track_columns_to_drop = [
        'track', 'track-artist', 'track_number', 'song_duration_ms', 'popularity',
        'danceability', 'danceability_category', 'energy', 'energy_category', 'loudness',
        'loudness_category', 'speechiness', 'speechiness_category', 'acousticness',
        'acousticness_category', 'instrumentalness', 'instrumentalness_category', 
        'liveness', 'liveness_category', 'valence', 'valence_category', 'key', 'key_name',
        'mode', 'mode_name', 'tempo', 'time_signature', 'first_year_listened'
    ]
    df.drop(columns=track_columns_to_drop, inplace=True, errors='ignore')

    # Drop unnecessary artist-related columns
    artist_columns_to_drop = [
        'artist', 'first_year_artist_listened', 'genres', 'genre1', 'genre2', 'genre3',
        'genre4', 'genre5', 'genre6', 'genre7', 'genre8', 'genre9', 'genre10', 
        'genre11', 'general_genre'
    ]
    df.drop(columns=artist_columns_to_drop, inplace=True, errors='ignore')

    # Merge with dim_album to add album_id
    df = df.merge(dim_album[['album_id', 'album', 'album-artist']], how='left', on='album-artist')
    df.drop(columns=['album_x', 'album_y', 'album-artist', 'album_id_x'], inplace=True, errors='ignore')
    df.rename(columns={'album_id_y': 'album_id'}, inplace=True)

    # Create composite key for dim_location
    dim_location['key'] = dim_location['conn_country'] + '-' + dim_location['ip_addr']
    df['key'] = df['conn_country'] + '-' + df['ip_addr']

    # Merge with dim_location to add location_id
    df = df.merge(dim_location[['location_id', 'key']], how='left', on='key')
    df.drop(columns=['conn_country_x', 'conn_country_y', 'ip_addr_x', 'ip_addr_y', 'key', 'location_id_x'], inplace=True, errors='ignore')
    df.rename(columns={'location_id_y': 'location_id'}, inplace=True)

    # Ensure unique_id is generated for new rows if not already present
    if 'unique_id' not in df.columns or df['unique_id'].isna().any():
        df['unique_id'] = df['unique_id'].fillna(pd.util.hash_pandas_object(df, index=False).astype(str))

    # Select and reorder relevant columns
    relevant_columns = [
        'unique_id', 'user_id', 'track_id', 'artist_id', 'album_id', 'location_id',
        'timestamp_listened', 'ms_listened', 'platform', 'reason_start', 'reason_end',
        'shuffle', 'incognito_mode'
    ]

    df = df[relevant_columns]

    # Drop duplicate rows to avoid redundancy
    df = df.drop_duplicates()

    return df

def get_spotify_tables(df, name):
    
    # create dimensions
    dim_user = get_dim_user(df, name)
    dim_track = get_dim_track(df,name)
    dim_artist = get_dim_artist(df,name)
    dim_album = get_dim_album(df,name)
    dim_location = get_dim_location(df,name)

    # create fact table
    
    fact_listening = get_fact_listening(df, name, dim_user, dim_album, dim_location)

    # create csv files
    tables = {
        "dim_user": dim_user,
        "dim_track": dim_track,
        "dim_artist": dim_artist,
        "dim_album": dim_album,
        "dim_location": dim_location,
        "fact_listening": fact_listening,
    }

    # Save each table as a CSV with its variable name
    for table_name, table in tables.items():
        table.to_csv(f'analysis_files/{name}/tables/{table_name}.csv', index=False)

    return None