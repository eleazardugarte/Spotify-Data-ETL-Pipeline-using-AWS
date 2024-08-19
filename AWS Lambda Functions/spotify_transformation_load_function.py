import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd

#Get album info from top 50 spotify songs
def album(data):
    album_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == "track":
                album = value['album']
                album_element = {'album_id': album['id'], 'name': album['name'], 'release_date': album['release_date'],
                                 'total_tracks': album['total_tracks'], 'url': album['external_urls']['spotify']}
                album_list.append(album_element)

    return album_list

#Get artists info from top 50 spotify songs
def artist(data):
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == "track":
                for artist in value['artists']:
                    artist_dict = {'artist_id': artist['id'], 'artist_name': artist['name'], 'external_url': artist['href']}
                    artist_list.append(artist_dict)

    return artist_list
#Get top 50 spotify songs
def songs(data):
    song_list = []
    for row in data['items']:
        song = row['track']
        song_element = {'song_id': song['id'], 'song_name': song['name'], 'duration_ms': song['duration_ms'], 'url': song['external_urls']['spotify'],
                            'popularity': song['popularity'], 'song_added': row['added_at'], 'album_id': song['album']['id'],
                            'artist_id': song['album']['artists'][0]['id']
                            }
        song_list.append(song_element)

    return song_list

#Main AWS lambda function
def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = 'spotify-etl-project-eleazar'
    path = 'raw_data/to_processed/'


    spotify_data = []
    spotify_keys = []
    for file in s3.list_objects(Bucket=Bucket, Prefix = path)['Contents']:
        file_key = file['Key']
        if file_key.endswith('.json'):
            response = s3.get_object(Bucket = Bucket, Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)

    print(spotify_keys)

    #loop for every song
    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        song_list = songs(data)

        album_df = pd.DataFrame.from_dict(album_list)
        album_df = album_df.drop_duplicates(subset=['album_id'])

        artist_df = pd.DataFrame.from_dict(artist_list)
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])

        song_df = pd.DataFrame.from_dict(song_list)

        album_df['release_date'] = pd.to_datetime(album_df['release_date'], errors='coerce')
        song_df['song_added'] = pd.to_datetime(song_df['song_added'], errors='coerce')

        songs_path = "transformed_data/songs_data/song_transformed_" + str(datetime.now()) + ".csv"
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index = False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=songs_path, Body = song_content)

        albums_path = "transformed_data/album_data/album_transformed_" + str(datetime.now()) + ".csv"
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index = False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=albums_path, Body = album_content)

        artist_path = "transformed_data/artist_data/artist_transformed_" + str(datetime.now()) + ".csv"
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index = False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_path, Body = artist_content)

    #Move files to processed
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
            }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split("/")[-1])
        s3_resource.Object(Bucket, key).delete()
