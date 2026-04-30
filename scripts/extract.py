import spotipy
import json
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyOAuth

# Garante que o Python encontre a pasta 'utils' na raiz do projeto
sys.path.append(os.path.abspath(os.path.join('..')))
from utils.minio_client import MinioClient

load_dotenv()

# autenticação com a api
client_credentials_manager = spotipy.oauth2.SpotifyClientCredentials(
    client_id= os.getenv("APP_CLIENT_ID"),
    client_secret= os.getenv("APP_CLIENT_SECRET"))
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# ____________________________________________________________________________________________________

# ID da banda - Oficina G3 
g3_uri = "0gO5Vbklho8yrBrUdHhuLH"

# Albums___
albums_g3 = []
offset = 0
limit = 10

#loop pra buscar todos os albuns da banda, já que a api está limitando buscar apenas 10 por vez
while True:
    # chamada a api
    results = sp.artist_albums(g3_uri, include_groups='album,single', limit=limit, offset=offset)
    items = results['items']
    # se não houver nenhum dado, encerra o loop
    if not items:
        break

    albums_g3.extend(items)
    offset += limit # aumenta limte da busca dos albums somando offset (indice de busca) com o limit
    
    # se o resultado atual for menor que o limite encerra o loop
    if len(items) < limit:
        break

print(f"Total de álbuns encontrados: {len(albums_g3)}")

# ____________________________________________________________________________________________________

#  Músicas___
tracks_g3 = []

# loop pra buscar músicas dos albúms do g3
for album in albums_g3:
    # busca os ids dos albums
    album_id = album['id']

    offset = 0
    limit = 50
    # busca das tracks dos albúms
    while True:
        track_g3 = sp.album_tracks(album_id, limit=limit, offset= offset, market='BR')
        tracks_in_album = track_g3['items']

        if not tracks_in_album:
            break

        for track in tracks_in_album:
            track['album_id'] = album_id
            tracks_g3.append(track)

        if len(track_g3) < limit:
            break

        offset += limit

print(f"Total de músicas encontradas: {len(tracks_g3)}")

# ____________________________________________________________________________________________________

minio = MinioClient()
bucket = os.getenv("BUCKET_NAME")
base_path = "../data/raw"
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Dicionario com chave e valor dos dados extraídos
dados_para_salvar = {
    "tracks_g3": tracks_g3,
    "albums_g3": albums_g3,
}

# loop pra salvar os jsons brutos
for folder_name, data in dados_para_salvar.items():

    # Caminho local:
    local_dir = os.path.join(base_path, folder_name)
    os.makedirs(local_dir, exist_ok=True)
    
    # Nome do arquivo local
    file_name = f"{folder_name}_{timestamp}.json"
    local_file_path = os.path.join(local_dir, file_name)

    # Salva o JSON localmente
    with open(local_file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        
    # Upload Bronze MinIO
    s3_object_name = f"bronze/{folder_name}/{file_name}"
    
    if minio.upload_file(bucket, s3_object_name, local_file_path):
        print(f"{folder_name} salvo: {bucket}/{s3_object_name}")