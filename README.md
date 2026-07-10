<img width="1536" height="1024" alt="spotify_data_pipeline_img" src="assets/spotify_data_pipeline_img.png" />

Pipeline de dados completo que extrai informações da API do Spotify sobre a banda **Oficina G3**, processa e armazena os dados em um datalake local, transforma em arquivos Parquet e carrega em um banco de dados PostgreSQL — tudo orquestrado com Airflow e rodando localmente via Docker. Os dados enriquecidos são explorados através de um **dashboard interativo em Streamlit**.

> 💡 Quer testar com outro artista? Basta substituir o `g3_uri = "0gO5Vbklho8yrBrUdHhuLH"` no extract.py pela URI do artista ou banda que você quiser buscar no Spotify.

Este é um projeto de estudo desenvolvido com o objetivo de aprender na prática, construindo um pipeline ETL real do zero. A ideia do projeto foi buscar dados da API do Spotify, passar por todas as camadas de um pipeline moderno - da extração bruta até o dado enriquecido e pronto para consulta, entender como cada ferramenta funciona dentro desse processo, tentando fazer tudo com o minimo de ajuda de IA possivel, apenas em momentos em que não enxerguei outra saida.

---

## App streamlit

![Dashboard gif](assets/spotify_data_pipeline_app.gif)

---

## Como Rodar o Projeto

### Pré-requisitos

- [Docker](https://www.docker.com/) instalado
- [DBeaver](https://dbeaver.io/download/) instalado (opcional, útil para explorar o banco além do dashboard)
- Credenciais da [API do Spotify](https://developer.spotify.com/dashboard) (Client ID e Client Secret)

### 1. Clone o repositório

```bash
git clone https://github.com/MIGUELEDL/spotify_data_pipeline.git
cd spotify_data_pipeline
```

### 2. Crie a estrutura de pastas locais

Essas pastas são geradas em runtime e não estão no repositório (`.gitignore`).
Crie-as antes de subir os serviços:

**Linux/macOS:**
```bash
mkdir -p data/airflow_logs data/minio_data data/postgres_data data/raw/albums_g3 data/raw/tracks_g3
```

**Windows (PowerShell):**
```powershell
New-Item -ItemType Directory -Force -Path data/airflow_logs, data/minio_data, data/postgres_data, data/raw/albums_g3, data/raw/tracks_g3
```

### 3. Configure as variáveis de ambiente

Crie um arquivo `.env` na raiz do projeto com suas credenciais:

```env
# AIRFLOW
AIRFLOW_IMAGE_NAME=apache/airflow:2.9.2
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=seu_usuario
_AIRFLOW_WWW_USER_PASSWORD=sua_senha

# Pastas locais montadas no container
AIRFLOW_DAGS=./dags
AIRFLOW_LOGS=./data/airflow_logs
AIRFLOW_PLUGINS=./plugins

# MINIO
MINIO_ENDPOINT=http://minio:9000
MINIO_ROOT_USER=seu_usuario
MINIO_ROOT_PASSWORD=sua_senha
BUCKET_NAME=spotify-data-pipeline

# API SPOTIFY
APP_CLIENT_ID=seu_client_id_aqui
APP_CLIENT_SECRET=seu_client_secret_aqui

# POSTGRES
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=seu_usuario
POSTGRES_PASSWORD=sua_senha
POSTGRES_DB=airflow
```

### 4. Suba os serviços com Docker

```bash
docker-compose up -d
```

### 5. Acesse as interfaces

| Serviço | URL | Credenciais padrão |
|---|---|---|
| Airflow | http://localhost:8080 | airflow / airflow |
| MinIO | http://localhost:9001 | minioadmin / minioadmin |
| **Dashboard (Streamlit)** | **http://localhost:8501** | — |

### 6. Ative a DAG no Airflow

Acesse o Airflow, localize a DAG "spotify_data_pipeline" e ative-a para iniciar o processo ETL.

### 7. Explore os dados no Dashboard

Com a DAG concluída, acesse **http://localhost:8501** para o dashboard interativo, construído em Streamlit sobre a camada `gold` do PostgreSQL. Ele traz:

- **Visão Geral** — KPIs da discografia, linha do tempo de lançamentos e destaques
- **Álbuns** — galeria de capas, filtro por década e detalhe de faixas por álbum
- **Faixas** — busca, filtros e distribuição de duração de todas as músicas
- **Rankings** — pódio e tabela com os álbuns mais longos, com mais faixas etc.
- **Evolução** — como a duração das faixas e o volume de lançamentos mudaram ao longo dos anos

Se preferir consultar o banco diretamente (ex: pra explorar queries SQL na mão), o DBeaver continua funcionando normalmente: crie uma conexão PostgreSQL com as credenciais do `.env` (Host: `localhost`, Porta: `5432`) e navegue até `airflow > esquemas > gold > tabelas`.

---

## Stack Utilizada:
| Ferramenta | Função |
|---|---|
| **Python** | Linguagem principal do pipeline |
| **Pandas** | Transformação e manipulação dos dados |
| **DuckDB** | Consultas SQL sobre os dados transformados |
| **Apache Airflow** | Orquestração e agendamento das DAGs |
| **MinIO** | Data Lake local (armazenamento de JSONs e Parquets) |
| **PostgreSQL** | Banco de dados para carga final dos dados |
| **Streamlit** | Dashboard interativo de visualização da camada gold |
| **Docker / Docker Compose** | Containerização de toda a infraestrutura |
| **SQL** | Enriquecimento e modelagem dos dados |

---

## Estrutura do Projeto

```
spotify_data_pipeline/
│
├── app/                          # Dashboard Streamlit
│   ├── pages/                    # Páginas do dashboard (Álbuns, Faixas, Rankings, Evolução)
│   ├── utils/                    # Cache de queries (db.py) e estilo/formatação (style.py)
│   └── app.py                    # Página inicial (Visão Geral)
│
├── dags/
│   └── spotify_dag_pipeline.py   # DAG principal do Airflow
│
├── data/                         # Dados gerados localmente (gitignore)
│   ├── airflow_logs/             # Logs do Airflow
│   ├── minio_data/               # Dados persistidos do MinIO
│   ├── postgres_data/            # Dados persistidos do PostgreSQL
│   └── raw/                      # JSONs brutos extraídos da API
│
├── scripts/                       # Core da lógica ETL e Notebooks de teste
│   ├── extract.py / .ipynb        # Extração da API Spotify -> JSON
│   ├── transform.py / .ipynb      # Limpeza e conversão JSON -> Parquet
│   ├── load.py / .ipynb           # Carga dos dados no PostgreSQL
│   └── init_gold_schema.sql       # Script de inicialização do banco
│
├── sql/
│   └── gold/                     # Queries SQL da camada enriquecida
│       ├── gold_albums_enriched.sql
│       ├── gold_discografia_summary.sql
│       ├── gold_evolucao_por_decada.sql
│       └── gold_tracks_enriched.sql
│
├── utils/
│   ├── minio_client.py           # Cliente de conexão com o MinIO
│   └── postgres_client.py        # Cliente de conexão com o PostgreSQL
│
├── .dockerignore
├── .env                          # Variáveis de ambiente
├── .gitignore
├── .python-version
├── docker-compose.yaml           # Orquestração dos serviços
├── Dockerfile                    # Imagem Docker customizada
├── pyproject.toml                # Dependências do projeto
├── uv.lock                       # Lock file do gerenciador uv
└── README.md
```

---

## O que Estou Aprendendo

Este projeto foi desenvolvido como parte da minha jornada de aprendizado em Engenharia de Dados. Cada ferramenta foi estudada e integrada com intenção, lendo documentações, entendendo como as libs funcionam por baixo dos panos e construindo a base antes de avançar para as etapas seguintes.

Algumas das habilidades praticadas:

- Consumo de APIs REST e autenticação OAuth2
- Armazenamento de dados brutos em formato JSON em um Data Lake
- Transformação de dados com Pandas e consultas com DuckDB
- Persistência em formato Parquet para otimização de leitura
- Modelagem e carga de dados em PostgreSQL
- Criação e orquestração de DAGs no Apache Airflow
- Construção de um dashboard interativo com Streamlit
- Containerização de toda a infraestrutura com Docker

---

## Autor

**Miguel Ernandes Dias Lucena**
Aspirante a Engenheiro de Dados | Caicó, RN

[![LinkedIn](https://img.shields.io/badge/-LINKEDIN-%230A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/miguel-ernandes-6b07b72a2/)
[![Substack](https://img.shields.io/badge/-SUBSTACK-%23FF6719?style=for-the-badge&logo=substack&logoColor=white)](https://substack.com/@migueledl)
[![Gmail](https://img.shields.io/badge/-GMAIL-%23333?style=for-the-badge&logo=gmail&logoColor=white)](mailto:miguelernandes2812@gmail.com)
[![Instagram](https://img.shields.io/badge/-INSTAGRAM-%23E4405F?style=for-the-badge&logo=instagram&logoColor=white)](https://www.instagram.com/miguelernandees/)

---

> *"Antes de manipular dados, você precisa entendê-los."*