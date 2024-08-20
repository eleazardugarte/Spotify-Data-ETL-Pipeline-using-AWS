import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd

#Edit the client_id and client_secret value provided by spotify API
client_credentials_manager = SpotifyClientCredentials(client_id="", client_secret="")
sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF"
playlist_URI = playlist_link.split("/")[-1]

data = sp.playlist_items(playlist_URI)

#Get album info
album_list = []
for row in data['items']:
    for key, value in row.items():
        if key == "track":
            album = value['album']
            album_element = {'album_id': album['id'], 'name': album['name'], 'release_date': album['release_date'],
                                 'total_tracks': album['total_tracks'], 'url': album['external_urls']['spotify']}
            album_list.append(album_element)

#Get top 50 artist info
artist_list = []
for row in data['items']:
    for key, value in row.items():
        if key == "track":
          for artist in value['artists']:
            artist_dict = {'artist_id': artist['id'], 'artist_name': artist['name'], 'external_url': artist['href']}
            artist_list.append(artist_dict)

#Get the spotify top 50 songs
song_list = []
for row in data['items']:
    song = row['track']
    song_element = {'song_id': song['id'], 'song_name': song['name'], 'duration_ms': song['duration_ms'], 'url': song['external_urls']['spotify'],
                            'popularity': song['popularity'], 'song_added': row['added_at'], 'album_id': song['album']['id'],
                            'artist_id': song['album']['artists'][0]['id']
                            }
    song_list.append(song_element)

print(song_list)