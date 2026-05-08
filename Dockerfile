FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /opt/airflow

# Copia dependências primeiro (cache do Docker)
COPY pyproject.toml uv.lock ./

# Instala tudo
RUN uv pip install --system --no-cache -r pyproject.toml

# Copia o código
COPY . .

ENV PYTHONUNBUFFERED=1
ENV AIRFLOW_HOME=/opt/airflow

# Inicializa o banco de metadados do Airflow e cria o usuário admin
# Isso roda apenas na primeira vez (standalone já faz isso, mas deixar explícito evita erros em ambientes limpos)
RUN airflow db migrate 2>/dev/null || true

CMD ["bash"]