-- Executado automaticamente pelo Postgres na primeira inicialização.
-- Cria o schema gold e garante permissões pro usuário do Airflow.

CREATE SCHEMA IF NOT EXISTS gold;
GRANT ALL PRIVILEGES ON SCHEMA gold TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL ON TABLES TO airflow;