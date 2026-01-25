import os
import re
import json
import requests
import redis
from pathlib import Path
from celery import Celery, chord
from celery.utils.log import get_task_logger
from celery.contrib.abortable import AbortableTask

# --- 設定 ---
# 環境に合わせて調整してください
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
OLLAMA_API_URL = os.getenv("OLLAMA_API_URL", "http://localhost:11434/api/generate")
NFS_BASE_PATH = Path("/data/")

logger = get_task_logger(__name__)

# Celeryインスタンスの設定
app = Celery("distributed_inference", broker=REDIS_URL, backend=REDIS_URL)
app.conf.update(
    task_routes={
        'tasks.split_and_dispatch': {'queue': 'parent_queue'},
        'tasks.aggregate_results': {'queue': 'parent_queue'},
        'tasks.process_chunk': {'queue': 'child_queue'},
    },
    result_expires=3600,
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
)

# Redisクライアント (進捗管理用)
redis_client = redis.StrictRedis.from_url(REDIS_URL, decode_responses=True)

# プロンプトテンプレート
SYSTEM_PROMPT_TEMPLATE = """
You are a professional {SOURCE_LANG} ({SOURCE_CODE}) to {TARGET_LANG} ({TARGET_CODE}) translator. Your goal is to accurately convey the meaning and nuances of the original {SOURCE_LANG} text while adhering to {TARGET_LANG} grammar, vocabulary, and cultural sensitivities.
Produce only the {TARGET_LANG} translation, without any additional explanations or commentary. Please translate the following {SOURCE_LANG} text into {TARGET_LANG}:


{TEXT}
"""

# --- ユーティリティ ---

def _extract_chunk_index(path_str: str) -> int:
    """ファイル名からインデックス番号を抽出する (数値ソート用)"""
    match = re.search(r'chunk_(\d+)\.txt$', path_str)
    return int(match.group(1)) if match else 999999

def get_task_progress(task_id: str):
    """外部のAPIやスクリプトから進捗状況を確認するためのユーティリティ"""
    total = redis_client.get(f"progress:{task_id}:total")
    completed = redis_client.get(f"progress:{task_id}:completed")
    
    if total is None:
        return {"status": "not_found"}
    
    total = int(total)
    completed = int(completed) if completed else 0
    percent = (completed / total * 100) if total > 0 else 0
    
    return {
        "task_id": task_id,
        "total": total,
        "completed": completed,
        "percent": round(percent, 2),
        "is_finished": completed >= total
    }

# --- タスク定義 ---

@app.task(bind=True, name='tasks.split_and_dispatch')
def split_and_dispatch(
    self, 
    file_path: str, 
    chunk_size: int = 50 * 1024,
    source_lang: str = "Japanese",
    source_code: str = "ja",
    target_lang: str = "English",
    target_code: str = "en"
):
    """
    【親タスク】ファイルを分割し、子タスクを管理・発行する
    """
    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    task_id = self.request.id
    temp_dir = NFS_BASE_PATH / "temp"
    temp_dir.mkdir(parents=True, exist_ok=True)

    chunk_paths = []
    
    # 1. ファイル分割処理
    with open(p, 'r', encoding='utf-8') as f:
        chunk_idx = 0
        current_lines = []
        current_bytes = 0

        for line in f:
            line_bytes = len(line.encode('utf-8'))
            if current_bytes + line_bytes > chunk_size and current_lines:
                # チャンク保存
                chunk_filename = f"{task_id}_chunk_{chunk_idx}.txt"
                cp = temp_dir / chunk_filename
                cp.write_text("".join(current_lines), encoding='utf-8')
                chunk_paths.append(str(cp))
                
                chunk_idx += 1
                current_lines = []
                current_bytes = 0

            current_lines.append(line)
            current_bytes += line_bytes

        # 残りのデータを保存
        if current_lines:
            chunk_filename = f"{task_id}_chunk_{chunk_idx}.txt"
            cp = temp_dir / chunk_filename
            cp.write_text("".join(current_lines), encoding='utf-8')
            chunk_paths.append(str(cp))

    if not chunk_paths:
        return {"status": "skipped", "reason": "empty file"}

    # 2. 進捗情報の初期化 (Redisに24時間保持)
    redis_client.setex(f"progress:{task_id}:total", 86400, len(chunk_paths))
    redis_client.setex(f"progress:{task_id}:completed", 86400, 0)

    # 3. Chord(並列処理)の構築
    header = [
        process_chunk.s(
            chunk_path=path,
            original_task_id=task_id,
            source_lang=source_lang,
            source_code=source_code,
            target_lang=target_lang,
            target_code=target_code
        ) for path in chunk_paths
    ]
    
    # 全子タスク完了後に実行されるコールバック
    callback = aggregate_results.s(original_task_id=task_id)
    
    chord(header)(callback)
    
    return {
        "status": "dispatched",
        "parent_task_id": task_id,
        "total_chunks": len(chunk_paths)
    }


@app.task(
    bind=True, 
    base=AbortableTask, 
    name='tasks.process_chunk',
    autoretry_for=(requests.exceptions.RequestException,),
    retry_kwargs={'max_retries': 3, 'countdown': 10}
)
def process_chunk(self, chunk_path: str, original_task_id: str, **lang_settings):
    """
    【子タスク】Ollamaを使用して翻訳を実行する
    """
    # 処理開始前の中断チェック
    if self.is_aborted():
        # 中断された場合でもカウントは進める（集約を止めてはいけないため）
        redis_client.incr(f"progress:{original_task_id}:completed")
        return {"status": "aborted", "path": chunk_path}

    try:
        # 1. ファイル読み込み
        path = Path(chunk_path)
        content = path.read_text(encoding='utf-8').strip()
        if not content:
            return {"status": "empty", "path": chunk_path}

        # 2. プロンプト作成
        full_prompt = SYSTEM_PROMPT_TEMPLATE.format(
            SOURCE_LANG=lang_settings.get("source_lang"),
            SOURCE_CODE=lang_settings.get("source_code"),
            TARGET_LANG=lang_settings.get("target_lang"),
            TARGET_CODE=lang_settings.get("target_code"),
            TEXT=content
        )

        # 3. Ollama APIリクエスト
        payload = {
            "model": "translategemma:12b",
            "prompt": full_prompt,
            "stream": False,
            "options": {"num_predict": 4096}  # 出力最大トークン数
        }
        
        response = requests.post(
            OLLAMA_API_URL, 
            json=payload, 
            timeout=600
        )
        response.raise_for_status()
        data = response.json()
        
        return {
            "input_path": chunk_path,
            "translated_text": data.get("response", ""),
            "status": "success"
        }

    except Exception as e:
        logger.error(f"Task {self.request.id} failed: {str(e)}")
        return {"status": "error", "message": str(e), "path": chunk_path}
    
    finally:
        # 成功・失敗・例外に関わらず完了数を1増やす
        redis_client.incr(f"progress:{original_task_id}:completed")


@app.task(name='tasks.aggregate_results')
def aggregate_results(results, original_task_id: str):
    """
    【集約タスク】すべての翻訳チャンクを結合し、成果物を保存する
    """
    results_dir = NFS_BASE_PATH / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    
    text_output_path = results_dir / f"{original_task_id}_final.txt"
    log_output_path = results_dir / f"{original_task_id}_log.json"

    # 1. チャンクを数値順にソート (chunk_10 が chunk_2 より後に来るように)
    sorted_results = sorted(
        results,
        key=lambda x: _extract_chunk_index(x.get('input_path', '') or x.get('path', ''))
    )

    combined_text = []
    temp_files_to_clean = []

    # 2. 結果の組み立てとクリーンアップ対象のリストアップ
    for res in sorted_results:
        path_key = res.get('input_path') or res.get('path')
        if path_key:
            temp_files_to_clean.append(path_key)

        if res.get("status") == "success":
            combined_text.append(res.get("translated_text", ""))
        else:
            # 失敗箇所をテキストに残す（エラーハンドリング用）
            combined_text.append(f"\n--- [ERROR IN CHUNK: {path_key}] ---\n")

    # 3. 成果物の書き出し
    text_output_path.write_text("\n".join(combined_text), encoding='utf-8')
    
    with open(log_output_path, 'w', encoding='utf-8') as f:
        json.dump(sorted_results, f, ensure_ascii=False, indent=2)

    # 4. 一時ファイルの削除
    for tmp_file in temp_files_to_clean:
        try:
            Path(tmp_file).unlink(missing_ok=True)
        except Exception as e:
            logger.warning(f"Cleanup failed for {tmp_file}: {e}")

    # 5. 進捗キーの有効期限を短縮 (完了後の整理)
    redis_client.expire(f"progress:{original_task_id}:total", 600)
    redis_client.expire(f"progress:{original_task_id}:completed", 600)

    return {
        "status": "completed",
        "final_file": str(text_output_path),
        "log_file": str(log_output_path),
        "total_processed": len(results)
    }