import os
from minio import Minio
from dotenv import load_dotenv

load_dotenv()

# autenticação com minio
class MinioClient:
    def __init__(self):
        endpoint = os.getenv("MINIO_ENDPOINT").replace("http://", "").replace("/", "")
        self.client = Minio(
            endpoint,
            access_key=os.getenv("MINIO_ROOT_USER"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
            secure=False
        )

    # função pra salvar arquivos dentro de buckets do minio
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