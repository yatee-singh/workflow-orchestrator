FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

COPY pyproject.toml README.md ./
COPY shared ./shared
COPY api ./api
COPY workers ./workers
COPY scripts ./scripts

RUN pip install --upgrade pip && pip install -e .

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
