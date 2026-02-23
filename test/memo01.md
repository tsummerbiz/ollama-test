ゴールデンプロンプト
以下のテキストをコピーして使用してください。

Markdown
# Role
あなたは分散システムとMLOps構築に精通したシニアソフトウェアエンジニアです。
FastAPI, Redis, Celeryを使用し、LLM（Ollama）と音声認識（Whisper）を統合した「高負荷対応・分散タスクキューシステム」を構築してください。

# System Requirement
1. **マイクロサービス構成**:
   - 推論サーバ（Ollama, Whisper）とタスクワーカー（Celery）は分離する。
   - アプリケーションコードは「ドメイン駆動」でディレクトリを分離する（TranslationとTranscription）。
   - ビルド環境は「コンテナ最適化」のため、Dockerfileとrequirements.txtを各役割ごとに分離する。

2. **Celery Configuration (重要)**:
   - **Queue**: 'translation' と 'transcription' の2つを定義。
   - **Concurrency**: 重いAI処理のため、1ワーカープロセスにつき「同時実行数 1 (Strict)」とする。
   - **Reliability**: `task_acks_late = True`, `worker_prefetch_multiplier = 1` を設定し、タスクの抱え込みとロストを防ぐ。

3. **Workers Architecture**:
   - Worker A: 翻訳専用 ('translation' queueのみ)
   - Worker B: 文字起こし専用 ('transcription' queueのみ)
   - Worker C & D: 兼用 (リソースが空いている時に両方のqueueを処理)

4. **Directory Structure**:
   以下の構造を厳守してコードを生成すること。
   ```text
   .
   ├── app/
   │   ├── __init__.py
   │   ├── config.py             # Celery設定 (include=['app.translation.tasks', ...])
   │   ├── main.py               # FastAPI entrypoint (Router統合)
   │   ├── translation/          # [翻訳ドメイン]
   │   │   ├── __init__.py
   │   │   ├── router.py         # POST /translate
   │   │   └── tasks.py          # Ollama API Client
   │   └── transcription/        # [文字起こしドメイン]
   │       ├── __init__.py
   │       ├── router.py         # POST /transcribe (ファイル保存処理含む)
   │       └── tasks.py          # Whisper API Client (/data 内のファイルパス処理)
   ├── data/                     # NFS共有用 (空で作成)
   ├── docker/                   # [Docker定義]
   │   ├── api.Dockerfile
   │   ├── translation.Dockerfile
   │   ├── transcription.Dockerfile
   │   └── general.Dockerfile
   ├── requirements/             # [依存関係]
   │   ├── common.txt            # Celery, Redis等
   │   ├── api.txt               # FastAPI等
   │   ├── translation.txt       # 翻訳用
   │   └── transcription.txt     # 音声処理用
   └── docker-compose.yml
```
Implementation Details
API Logic:

/translate: テキストを受け取りOllama APIへ。

/transcribe: 音声ファイルを受け取り、共有ボリューム /data に保存した後、パスをWhisper APIへ渡す。

Docker Compose:

build context はルート(.)を指定し、dockerfile パスで各docker/配下のファイルを指定すること。

OllamaとWhisperのサービス定義を含める（Whisperは簡易的に python -m http.server 等のダミーでも可だが、接続設定は記述すること）。

Output
上記のディレクトリ構造に従い、以下のファイルの全コードを生成してください。

requirements/ 配下の全ファイル

docker/ 配下の全Dockerfile

docker-compose.yml

app/config.py, app/main.py

app/translation/ 配下のコード

app/transcription/ 配下のコード


---

### プロンプトの解説（エンジニア向け）

このプロンプトには、意図通りの出力を得るための「トリガー」が仕込まれています。

1.  **Directory Structureの図示**:
    * AIにファイルパスを誤認させないため、ツリー構造を明示しました。これにより、`app/translation/tasks.py` といった深い階層のファイルも正確に生成されます。
2.  **`build context` の指定**:
    * Docker分離構成で最もハマりやすい「ContextはルートだがDockerfileはサブディレクトリ」という設定を、`Implementation Details` で明示的に指示しています。
3.  **Configの `include` 指定**:
    * `app/config.py` 内で `include=['app.translation.tasks', ...]` と書かせないと、Celeryがタスクを認識できない問題を未然に防いでいます。
4.  **各ワーカーの役割定義**:
    * Worker A/B/C/D のキュー割り当て（`-Q` オプション）を `docker-compose.yml` に正しく反映させるための指示です。

---
---
承知いたしました。シニアソフトウェアエンジニアとして、スケーラビリティと保守性を兼ね備えた、FastAPI, Redis, Celery, Ollama, Whisperによる分散タスクキューシステムの全コードを構築します。

この構成は、特にGPUリソースを大量に消費するAI推論において、ワーカーの専有と共有を動的に制御する実践的な設計です。

---

## 1. Requirements (依存関係)

### requirements/common.txt

```text
celery==5.3.6
redis==5.0.1
pydantic==2.5.2
python-dotenv==1.0.0

```

### requirements/api.txt

```text
-r common.txt
fastapi==0.104.1
uvicorn==0.24.0
python-multipart==0.0.6

```

### requirements/translation.txt

```text
-r common.txt
requests==2.31.0

```

### requirements/transcription.txt

```text
-r common.txt
requests==2.31.0

```

---

## 2. Docker Definitions

### docker/api.Dockerfile

```dockerfile
FROM python:3.10-slim
WORKDIR /src
COPY requirements/api.txt .
COPY requirements/common.txt .
RUN pip install --no-cache-dir -r api.txt
COPY . .
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

```

### docker/translation.Dockerfile

```dockerfile
FROM python:3.10-slim
WORKDIR /src
COPY requirements/translation.txt .
COPY requirements/common.txt .
RUN pip install --no-cache-dir -r translation.txt
COPY . .
CMD ["celery", "-A", "app.config.celery_app", "worker", "--loglevel=info"]

```

### docker/transcription.Dockerfile

```dockerfile
FROM python:3.10-slim
WORKDIR /src
COPY requirements/transcription.txt .
COPY requirements/common.txt .
RUN pip install --no-cache-dir -r transcription.txt
COPY . .
CMD ["celery", "-A", "app.config.celery_app", "worker", "--loglevel=info"]

```

### docker/general.Dockerfile

```dockerfile
FROM python:3.10-slim
WORKDIR /src
COPY requirements/translation.txt .
COPY requirements/transcription.txt .
COPY requirements/common.txt .
RUN pip install --no-cache-dir -r translation.txt -r transcription.txt
COPY . .
CMD ["celery", "-A", "app.config.celery_app", "worker", "--loglevel=info"]

```

---

## 3. Docker Compose

### docker-compose.yml

```yaml
version: '3.8'

services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  api:
    build:
      context: .
      dockerfile: docker/api.Dockerfile
    ports:
      - "8000:8000"
    environment:
      - CELERY_BROKER_URL=redis://redis:6379/0
      - CELERY_RESULT_BACKEND=redis://redis:6379/0
    volumes:
      - ./data:/data
    depends_on:
      - redis

  worker-translation-a:
    build:
      context: .
      dockerfile: docker/translation.Dockerfile
    command: celery -A app.config.celery_app worker -Q translation --hostname=worker-a@%h
    environment:
      - CELERY_BROKER_URL=redis://redis:6379/0
      - OLLAMA_URL=http://ollama:11434
    depends_on:
      - redis

  worker-transcription-b:
    build:
      context: .
      dockerfile: docker/transcription.Dockerfile
    command: celery -A app.config.celery_app worker -Q transcription --hostname=worker-b@%h
    volumes:
      - ./data:/data
    environment:
      - CELERY_BROKER_URL=redis://redis:6379/0
      - WHISPER_URL=http://whisper:9000
    depends_on:
      - redis

  worker-mixed-cd:
    build:
      context: .
      dockerfile: docker/general.Dockerfile
    deploy:
      replicas: 2
    command: celery -A app.config.celery_app worker -Q translation,transcription --hostname=worker-mixed@%h
    volumes:
      - ./data:/data
    environment:
      - CELERY_BROKER_URL=redis://redis:6379/0
      - OLLAMA_URL=http://ollama:11434
      - WHISPER_URL=http://whisper:9000
    depends_on:
      - redis

  ollama:
    image: ollama/ollama:latest
    ports:
      - "11434:11434"

  whisper:
    image: onerahmet/openai-whisper-asr-webservice:latest
    ports:
      - "9000:9000"
    environment:
      - ASR_MODEL=base

```

---

## 4. Application Core

### app/config.py

```python
import os
from celery import Celery

REDIS_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")

celery_app = Celery(
    "tasks",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=[
        "app.translation.tasks",
        "app.transcription.tasks"
    ]
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    # System Requirements
    worker_concurrency=1,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    task_queues={
        "translation": {"exchange": "translation", "routing_key": "translation"},
        "transcription": {"exchange": "transcription", "routing_key": "transcription"},
    }
)

```

### app/main.py

```python
from fastapi import FastAPI
from app.translation.router import router as translation_router
from app.transcription.router import router as transcription_router

app = FastAPI(title="Distributed AI Task Queue")

app.include_router(translation_router, prefix="/translate", tags=["Translation"])
app.include_router(transcription_router, prefix="/transcribe", tags=["Transcription"])

@app.get("/")
async def root():
    return {"status": "online", "message": "MLOps Distributed System API"}

```

---

## 5. Domain Logic

### app/translation/router.py

```python
from fastapi import APIRouter
from pydantic import BaseModel
from .tasks import run_translation_task

router = APIRouter()

class TranslationRequest(BaseModel):
    text: str
    target_lang: str = "Japanese"

@router.post("/")
async def translate(request: TranslationRequest):
    task = run_translation_task.delay(request.text, request.target_lang)
    return {"task_id": task.id, "status": "Queued"}

@router.get("/result/{task_id}")
async def get_result(task_id: str):
    from app.config import celery_app
    res = celery_app.AsyncResult(task_id)
    return {"status": res.status, "result": res.result if res.ready() else None}

```

### app/translation/tasks.py

```python
import os
import requests
from app.config import celery_app

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")

@celery_app.task(queue="translation", name="app.translation.tasks.run_translation_task")
def run_translation_task(text, target_lang):
    prompt = f"Translate the following text to {target_lang}: {text}"
    
    response = requests.post(
        f"{OLLAMA_URL}/api/generate",
        json={
            "model": "llama3",
            "prompt": prompt,
            "stream": False
        }
    )
    response.raise_for_status()
    return response.json().get("response")

```

### app/transcription/router.py

```python
import os
import uuid
from fastapi import APIRouter, UploadFile, File
from .tasks import run_transcription_task

router = APIRouter()
DATA_DIR = "/data"

@router.post("/")
async def transcribe(file: UploadFile = File(...)):
    file_ext = file.filename.split(".")[-1]
    file_path = os.path.join(DATA_DIR, f"{uuid.uuid4()}.{file_ext}")
    
    with open(file_path, "wb") as f:
        f.write(await file.read())
    
    task = run_transcription_task.delay(file_path)
    return {"task_id": task.id, "file_path": file_path, "status": "Queued"}

```

### app/transcription/tasks.py

```python
import os
import requests
from app.config import celery_app

WHISPER_URL = os.getenv("WHISPER_URL", "http://localhost:9000")

@celery_app.task(queue="transcription", name="app.transcription.tasks.run_transcription_task")
def run_transcription_task(file_path):
    if not os.path.exists(file_path):
        return "Error: File not found in shared volume."

    with open(file_path, "rb") as f:
        files = {"audio_file": f}
        response = requests.post(f"{WHISPER_URL}/asr", files=files)
    
    response.raise_for_status()
    # 完了後にファイルを削除する場合はここで実行
    # os.remove(file_path)
    return response.json().get("text")

```

---

### 構築のポイント

1. **Strict Concurrency**: 各ワーカーの `concurrency` を `1` に制限することで、GPUメモリの競合を回避しています。
2. **Shared Storage**: 音声ファイルは `/data` ボリューム経由で API サーバーからワーカーへ受け渡されます。NFS等を想定した設計です。
3. **Flexible Routing**: `docker-compose` 内で `-Q` フラグを使い分けることで、専用機と汎用機のクラスタリングを実現しました。

このシステムを起動するには、ルートディレクトリで `docker-compose up --build` を実行してください。次に何かお手伝いできることはありますか？例えば、K8sへのデプロイマニフェスト作成や、タスク監視ツールのFlowerの追加など可能です。


