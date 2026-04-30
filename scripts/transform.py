import pandas as pd
import sys
import os
from datetime import datetime
from dotenv import load_dotenv

# Garante que o Python encontre a pasta 'utils' na raiz do projeto
sys.path.append(os.path.abspath(os.path.join('..')))
from utils.minio_client import MinioClient

load_dotenv() # carrega .env

minio = MinioClient()
bucket = os.getenv("BUCKET_NAME")
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# pegando os últimos arquivos json salvos de cada pasta da camada bronze
albums_g3 = minio.get_ultimo_arquivo(bucket, "bronze/albums_g3")
tracks_g3 = minio.get_ultimo_arquivo(bucket, "bronze/tracks_g3")

# ____________________________________________________________________________________________________

# Transformação dos albums___
def transform_albums(data) -> pd.DataFrame:
    """
    Transforma os dados brutos dos albums da Bronze.
    """
    items = data["items"] if isinstance(data, dict) and "items" in data else data

    # achata tudo, criando colunas, pois, o JSON da API do Spotify tem campos dentro de campos
    df_album = pd.json_normalize(items)

    # renomeando colunas
    df_album = df_album.rename(columns={
        "id":                    "id_album",
        "total_tracks":          "qtd_faixas",
        "name":                  "nome_album",
        "release_date":          "data_lançamento",
        "images":                "capa_album",
        "external_urls.spotify": "urls_externos",
    })

    # separando tamanho das imagens das capas dos albums
    df_album['capa_album_640'] = df_album['capa_album'].apply(lambda x: x[0]['url'] if isinstance(x, list) and len(x) > 0 else None)
    df_album['capa_album_300'] = df_album['capa_album'].apply(lambda x: x[1]['url'] if isinstance(x, list) and len(x) > 1 else None)
    df_album['capa_album_64']  = df_album['capa_album'].apply(lambda x: x[2]['url'] if isinstance(x, list) and len(x) > 2 else None)

    # selecionando as apenas colunas úteis
    df_album = df_album[[
        "id_album", "qtd_faixas", "nome_album", 
        "data_lançamento", "capa_album_640", 
        "capa_album_300", "capa_album_64"
    ]]

    # Remove duplicatas e definindo coluna de data
    df_album = df_album.drop_duplicates(subset="id_album")
    df_album["data_lançamento"] = pd.to_datetime(df_album["data_lançamento"], format='mixed', errors='coerce')

    return df_album

# ____________________________________________________________________________________________________

# Transformação dos albums___
def transform_tracks(data) -> pd.DataFrame:
    """
    Transforma os dados brutos das tracks da Bronze.
    """
    items = data["items"] if isinstance(data, dict) and "items" in data else data

    # achata tudo, criando colunas, pois, o JSON da API do Spotify tem campos dentro de campos
    df_track = pd.json_normalize(tracks_g3)

    # renomeando colunas
    df_track = df_track.rename(columns={
        "id":                    "id_musica",
        "name":                  "nome_musica",
        "track_number":          "numero_no_album",
        "is_playable":           "Reproduzivel",
        "duration_ms":           "duracao_ms",
        "explicit":              "conteudo_explicito",
        "external_urls.spotify": "url_da_musica",
        "album_id":              "id_album",
    })

    # Remove duplicatas e cria coluna com minuto e segundos em strings formatadas pra melhor visualização
    df_track = df_track.drop_duplicates(subset="nome_musica")
    df_track["duracao_min_seg"] = pd.to_datetime(df_track['duracao_ms'], unit='ms').dt.strftime('%M:%S')

    # selecionando as apenas as colunas úteis
    df_track = df_track[[
        "id_musica", "nome_musica", "duracao_min_seg", "numero_no_album", "Reproduzivel", 
        "conteudo_explicito", "duracao_ms", "url_da_musica", "id_album",
    ]]

    return df_track

# ____________________________________________________________________________________________________

# aplicando as funções de tratamento e transformando em dataframe
df_albums_g3 = transform_albums(albums_g3)
df_tracks_g3 = transform_tracks(tracks_g3)

# Salva na Silver como Parquet
minio.upload_parquet(bucket, f"silver/albums_g3/albums_g3(transform)_{timestamp}.parquet", df_albums_g3)
minio.upload_parquet(bucket, f"silver/tracks_g3/tracks_g3(transform)_{timestamp}.parquet", df_tracks_g3)