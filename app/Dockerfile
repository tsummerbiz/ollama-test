# --- Stage 1: Build ---
FROM python:3.12-slim AS builder

# uv のバイナリをコピー
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# requirements.txt をコピー
COPY requirements.txt .

# 仮想環境を作成し、uv pip でインストール
# --system を使わず .venv を作成してコピーする構成を維持します
RUN --mount=type=cache,target=/root/.cache/uv \
    uv venv /app/.venv && \
    uv pip install -r requirements.txt

# --- Stage 2: Run ---
FROM python:3.12-slim

WORKDIR /app

# ビルドステージから .venv をコピー
COPY --from=builder /app/.venv /app/.venv

# アプリケーションコードをコピー
COPY . .

# 環境変数の設定
ENV PATH="/app/.venv/bin:$PATH"

#CMD ["python", "main.py"]