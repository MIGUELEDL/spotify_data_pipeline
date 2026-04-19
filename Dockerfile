# Python leve (Slim) para diminuir o tamanho final
FROM python:3.12-slim

# Instala dependências do sistema necessárias
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Instala o uv diretamente do binário oficial para máxima velocidade
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Copia os arquivos de dependências primeiro (otimiza o cache do Docker)
COPY pyproject.toml uv.lock ./

# Instala as dependências do projeto usando o uv
# --system garante que ele instale no ambiente global do container
RUN uv pip install --system --no-cache -r pyproject.toml

# Copia o restante do código do projeto
COPY . .

# Define variáveis de ambiente para o Airflow e Python
ENV PYTHONUNBUFFERED=1
ENV AIRFLOW_HOME=/opt/airflow

# O comando final é sobrescrito pelo docker-compose, 
# mas deixamos um padrão por segurança
CMD ["bash"]