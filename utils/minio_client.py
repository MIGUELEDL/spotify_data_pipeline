import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()

class MinioClient:
    # faz a autenticação com o minio
    def __init__(self):
        endpoint = os.getenv("MINIO_ENDPOINT").replace("http://", "").replace("/", "")
        self.client = Minio(
            endpoint,
            access_key=os.getenv("MINIO_ROOT_USER"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
            secure=False,
        )

    # função pra salvar arquivos json dentro de buckets do minio
    def upload_file(self, bucket_name, object_name, file_path):
        """Faz o upload de um arquivo para o bucket especificado."""
        try:
            # Garante que o bucket existe antes de subir
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                print(f"Bucket '{bucket_name}' criado com sucesso.")

            self.client.fput_object(bucket_name, object_name, file_path)
            return True
        except Exception as e:
            print(f"Erro no upload para o MinIO: {e}")
            return False
    
    #_____________________________________________________________________________________________________________#

    # função pra buscar último arquivo salvo de buckets no minio
    def get_ultimo_arquivo(self, bucket_name: str, folder: str, formato: str = "json") -> dict | list | pd.DataFrame:
        """
        Busca o arquivo mais recente de uma pasta dentro do bucket.
        Suporta JSON (camada bronze) e Parquet (camada silver).

        Exemplos:
            minio.get_ultimo_arquivo(bucket, "bronze/artists") # JSON, padrão
            minio.get_ultimo_arquivo(bucket, "silver/artists", "parquet") # Parquet
        """
        try:
            # Lista todos os objetos da pasta
            objects = list(
                self.client.list_objects(bucket_name, prefix=f"{folder}/", recursive=True)
            )

            # Filtra pelo formato solicitado
            arquivos = [obj for obj in objects if obj.object_name.endswith(f".{formato}")]

            if not arquivos:
                print(f"Nenhum arquivo {formato.upper()} encontrado em: {folder}/")
                return None

            # Pega o mais recente pelo last_modified
            latest = max(arquivos, key=lambda obj: obj.last_modified)
            print(f"{latest.object_name} | {latest.last_modified.strftime('%Y-%m-%d %H:%M:%S')}")

            # Lê e retorna o conteúdo bruto
            response = self.client.get_object(bucket_name, latest.object_name)
            conteudo = response.read()

            # Retorna conforme o formato
            if formato == "json":
                return json.loads(conteudo.decode("utf-8"))
            
            elif formato == "parquet":
                buffer = BytesIO(conteudo)
                return pq.read_table(buffer).to_pandas()

        except Exception as e:
            print(f"Erro ao buscar arquivo em '{folder}/': {e}")
            return None

    #_____________________________________________________________________________________________________________#

    # função pra salvar DataFrames como Parquet na camada silver
    def upload_parquet(self, bucket_name: str, object_name: str, df: pd.DataFrame) -> bool:
        """
        Converte um DataFrame para Parquet e faz upload direto para o MinIO,
        sem precisar salvar o arquivo localmente.

        Exemplo:
            minio.upload_parquet(bucket, "silver/artists/artists_20260424.parquet", df_artists)
        """
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                print(f"Bucket '{bucket_name}' criado com sucesso.")

            # Converte o DataFrame para Parquet em memória
            buffer = BytesIO()
            table = pa.Table.from_pandas(df)
            pq.write_table(table, buffer, compression="snappy")

            # Volta o cursor do buffer para o início antes de fazer upload
            buffer.seek(0)

            # envia para o MinIO
            self.client.put_object(
                bucket_name,
                object_name,
                data=buffer,
                length=buffer.getbuffer().nbytes,
                content_type="application/octet-stream"
            )
            
            print(f"Parquet salvo em: {bucket_name}/{object_name}")
            return True

        except Exception as e:
            print(f"Erro ao salvar Parquet em '{object_name}': {e}")
            return False
