import os
import re
import json
import requests
import redis
import hashlib
from pathlib import Path
from celery import Celery, chord
from celery.utils.log import get_task_logger
from celery.contrib.abortable import AbortableTask

# 暗号化ライブラリ
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend

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

# --- 暗号化・復号ヘルパー ---

def get_cipher_key(password: str) -> bytes:
    """パスワードから32バイトの鍵を生成"""
    return hashlib.sha256(password.encode()).digest()

def encrypt_and_save(content: str, file_path: Path, password: str):
    """テキストを暗号化してバイナリ保存"""
    key = get_cipher_key(password)
    iv = os.urandom(16)
    
    padder = padding.PKCS7(128).padder()
    padded_data = padder.update(content.encode('utf-8')) + padder.finalize()
    
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()
    encrypted_content = encryptor.update(padded_data) + encryptor.finalize()
    
    file_path.write_bytes(iv + encrypted_content)

def decrypt_file(file_path: Path, password: str) -> str:
    """暗号化されたファイルを復号してテキストを返す"""
    if not file_path.exists():
        return ""
    
    data = file_path.read_bytes()
    iv = data[:16]
    encrypted_content = data[16:]
    
    key = get_cipher_key(password)
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()
    
    padded_data = decryptor.update(encrypted_content) + decryptor.finalize()
    
    unpadder = padding.PKCS7(128).unpadder()
    content = unpadder.update(padded_data) + unpadder.finalize()
    
    return content.decode('utf-8')

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

def get_callback_id(task_id: str):
    # 1. RedisからコールバックタスクのIDを取得
    callback_id = redis_client.get(f"callback_map:{task_id}")
    
    # マッピングがない場合は、直接 task_id を使用（以前の互換性のため）
    target_id = callback_id if callback_id else task_id

    return target_id

# --- タスク定義 ---

@app.task(bind=True, name='tasks.split_and_dispatch')
def split_and_dispatch(
    self, 
    file_path: str, 
    encryption_password: str = "password",
    chunk_size: int = 8 * 1024, #上限 128 * 1024
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
            encryption_password=encryption_password,
            source_lang=source_lang,
            source_code=source_code,
            target_lang=target_lang,
            target_code=target_code
        ) for path in chunk_paths
    ]
    
    # 全子タスク完了後に実行されるコールバック
    callback = aggregate_results.s(
        original_task_id=task_id, 
        encryption_password=encryption_password
    )
    
    # chord を実行し、その AsyncResult を取得
    chord_result = chord(header)(callback)

    # 【重要】親IDからコールバックIDを引けるようにRedisに保存 (有効期限は24時間)
    redis_client.setex(f"callback_map:{task_id}", 86400, chord_result.id)

    return {
        "status": "dispatched",
        "parent_task_id": task_id,
        "callback_task_id": chord_result.id, 
        "total_chunks": len(chunk_paths)
    }


@app.task(
    bind=True, 
    base=AbortableTask, 
    name='tasks.process_chunk',
    autoretry_for=(requests.exceptions.RequestException,),
    # タスクが終了してから次のタスクを取得するようにする
    acks_late=True,
    # Workerが一度に取得するタスクの数を厳密に制限
    worker_prefetch_multiplier=1,
    retry_kwargs={'max_retries': 3, 'countdown': 30}
)
def process_chunk(
    self, 
    chunk_path: str, 
    original_task_id: str, 
    encryption_password: str = "password",
    **lang_settings
):
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
#            "model": "translategemma:12b",
            "model": "translategemma",
            "prompt": full_prompt,
            "stream": False
#            "options": {"num_predict": 4096}  # 出力最大トークン数
        }
        
        response = requests.post(
            OLLAMA_API_URL, 
            json=payload, 
            timeout=60*60
        )
        response.raise_for_status()
        data = response.json()
        translated_text =  data.get("response", "")

        # 結果を「暗号化された一時ファイル」として保存
        result_chunk_path = path.with_suffix('.translated.enc')
        encrypt_and_save(translated_text, result_chunk_path, encryption_password)
        
        return {
            "input_path": chunk_path,
            "result_path": str(result_chunk_path),
            "status": "success"
        }

    except Exception as e:
        logger.error(f"Task {self.request.id} failed: {str(e)}")
        return {"status": "error", "message": str(e), "path": chunk_path}
    
    finally:
        # 成功・失敗・例外に関わらず完了数を1増やす
        redis_client.incr(f"progress:{original_task_id}:completed")


@app.task(name='tasks.aggregate_results')
def aggregate_results(
    results, 
    original_task_id: str,
    encryption_password: str = "password"
):
    """
    【集約タスク】すべての翻訳チャンクを結合し、成果物を保存する
    """
    results_dir = NFS_BASE_PATH / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    
    text_output_path = results_dir / f"{original_task_id}_final.txt"
    log_output_path = results_dir / f"{original_task_id}_log.json"

    final_output_path = results_dir / f"{original_task_id}_final.txt.enc"
    
    # 1. チャンクを数値順にソート (chunk_10 が chunk_2 より後に来るように)
    sorted_results = sorted(
        results,
        key=lambda x: _extract_chunk_index(x.get('input_path', '') or x.get('path', ''))
    )

    combined_text = []
    files_to_clean = []

    # 2. 結果の組み立てとクリーンアップ対象のリストアップ
    for res in sorted_results:
        in_path = res.get('input_path')
        res_path = res.get('result_path')
        
        if in_path: files_to_clean.append(Path(in_path))
        
        if res.get("status") == "success" and res_path:
            # 2. 中間ファイルを復号して読み込み
            p = Path(res_path)
            text = decrypt_file(p, encryption_password)
            combined_text.append(text)
            files_to_clean.append(p)
        else:
            combined_text.append(f"\n--- [ERROR IN CHUNK] ---\n")

    # 3. 結合したテキストを最終暗号化して保存
    final_content = "\n".join(combined_text)
    encrypt_and_save(final_content, final_output_path, encryption_password)
    
    with open(log_output_path, 'w', encoding='utf-8') as f:
        json.dump(sorted_results, f, ensure_ascii=False, indent=2)

    """
    # 4. 一時ファイルの削除
    for f in files_to_clean:
        try:
            f.unlink(missing_ok=True)
        except Exception as e:
            logger.warning(f"Cleanup failed for {tmp_file}: {e}")
    """
    # 5. 進捗キーの有効期限を短縮 (完了後の整理)
    redis_client.expire(f"progress:{original_task_id}:total", 60*60)
    redis_client.expire(f"progress:{original_task_id}:completed", 60*60)

    return {
        "status": "completed",
        "final_file": str(final_output_path),
        "log_file": str(log_output_path),
        "total_processed": len(results)
    }