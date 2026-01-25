import os
import json
import requests
from celery import Celery, chord
from celery.contrib.abortable import AbortableTask

# 環境変数
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
OLLAMA_API_URL = os.getenv("OLLAMA_API_URL", "http://localhost:11434/api/generate")
NFS_BASE_PATH = "/data/"

app = Celery("distributed_inference", broker=REDIS_URL, backend=REDIS_URL)

# タスクのルーティング設定 (重要: 親と子でキューを分ける)
app.conf.task_routes = {
    'tasks.split_and_dispatch': {'queue': 'parent_queue'},
    'tasks.aggregate_results': {'queue': 'parent_queue'},
    'tasks.process_chunk': {'queue': 'child_queue'},
}

# 結果の保持時間を設定 (Redisの肥大化防止)
app.conf.result_expires = 3600

@app.task(bind=True)
def split_and_dispatch(
    self, 
    file_path: str, 
    chunk_size: int = 50 * 1024,  # デフォルトを50KBに設定（モデルのコンテキスト制限を考慮）
    source_lang: str = "Japanese",
    source_code: str = "ja",
    target_lang: str = "English",
    target_code: str = "en"
):
    """
    【親タスク】ファイルをバイト数ベースで行単位分割し、翻訳情報を添えて子タスクを発行する
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    task_id = self.request.id
    chunk_paths = []
    
    # 一時保存ディレクトリの作成
    temp_dir = os.path.join(NFS_BASE_PATH, "temp")
    os.makedirs(temp_dir, exist_ok=True)

    # 1. ファイルを読み込み、バイト数ベースで分割
    with open(file_path, 'r', encoding='utf-8') as f:
        chunk_idx = 0
        current_chunk_lines = []
        current_chunk_bytes = 0

        for line in f:
            line_bytes = len(line.encode('utf-8'))

            # 現在の行を加えるとchunk_sizeを超える場合（かつ既にデータがある場合）、書き出し
            if current_chunk_bytes + line_bytes > chunk_size and current_chunk_lines:
                chunk_path = _save_chunk(task_id, chunk_idx, current_chunk_lines)
                chunk_paths.append(chunk_path)
                
                chunk_idx += 1
                current_chunk_lines = []
                current_chunk_bytes = 0

            current_chunk_lines.append(line)
            current_chunk_bytes += line_bytes

        # 最後の残りデータを書き出し
        if current_chunk_lines:
            chunk_path = _save_chunk(task_id, chunk_idx, current_chunk_lines)
            chunk_paths.append(chunk_path)

    # 2. Chordの構築
    # 集約用タスクに元のタスクIDを渡す
    callback = aggregate_results.s(original_task_id=task_id)
    
    # 各子タスクに、分割ファイルのパスと言語設定を渡す
    header = [
        process_chunk.s(
            chunk_path=path,
            source_lang=source_lang,
            source_code=source_code,
            target_lang=target_lang,
            target_code=target_code
        ) for path in chunk_paths
    ]
    
    # 3. 非同期実行開始
    if header:
        chord(header)(callback)
    else:
        # ファイルが空だった場合などの処理
        return {"status": "skipped", "reason": "empty file"}
    
    return {
        "status": "dispatched", 
        "chunks": len(chunk_paths),
        "source": source_lang,
        "target": target_lang
    }

def _save_chunk(task_id, idx, lines):
    """チャンクをファイルに保存するヘルパー関数"""
    chunk_filename = f"{task_id}_chunk_{idx}.txt"
    chunk_path = os.path.join(NFS_BASE_PATH, "temp", chunk_filename)
    with open(chunk_path, 'w', encoding='utf-8') as cf:
        cf.writelines(lines)
    return chunk_path


# プロンプトテンプレート（固定文字列）
SYSTEM_PROMPT_TEMPLATE = """You are a professional {SOURCE_LANG} ({SOURCE_CODE}) to {TARGET_LANG} ({TARGET_CODE}) translator. Your goal is to accurately convey the meaning and nuances of the original {SOURCE_LANG} text while adhering to {TARGET_LANG} grammar, vocabulary, and cultural sensitivities.
Produce only the {TARGET_LANG} translation, without any additional explanations or commentary. Please translate the following {SOURCE_LANG} text into {TARGET_LANG}:

{TEXT}"""

@app.task(bind=True, base=AbortableTask)
def process_chunk(self, chunk_path: str, source_lang="Japanese", source_code="ja", target_lang="English", target_code="en"):
    """
    【子タスク】チャンクファイルを一括で読み込み、プロンプトを付与してOllamaで推論を行う。
    """
    # 処理開始前のキャンセルチェック
    if self.is_aborted():
        return {"status": "aborted", "path": chunk_path}

    try:
        # 1. ファイルを一括読み込み
        with open(chunk_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()

        if not content:
            return {"status": "empty", "path": chunk_path}

        # 2. プロンプトの組み立て
        full_prompt = SYSTEM_PROMPT_TEMPLATE.format(
            SOURCE_LANG=source_lang,
            SOURCE_CODE=source_code,
            TARGET_LANG=target_lang,
            TARGET_CODE=target_code,
            TEXT=content
        )

        # 3. 中断チェック（APIリクエスト直前）
        if self.is_aborted():
            return {"status": "aborted", "path": chunk_path}

        # 4. Ollamaへのリクエスト
        payload = {
            "model": "translategemma:12b",
            "prompt": full_prompt,
            "stream": False
        }
        
        response = requests.post(
            OLLAMA_API_URL, 
            json=payload, 
            timeout=600  # 一括処理は時間がかかるためタイムアウトを長めに設定
        )
        response.raise_for_status()
        data = response.json()
        
        # 5. 結果の返却
        return {
            "input_path": chunk_path,
            "translated_text": data.get("response", ""),
            "status": "success"
        }

    except Exception as e:
        return {"status": "error", "message": str(e), "path": chunk_path}

import os
import json

@app.task
def aggregate_results(results, original_task_id: str):
    """
    【集約タスク】子タスク（辞書形式）の結果を結合して保存する
    """
    # 保存先パスの設定
    results_dir = os.path.join(NFS_BASE_PATH, "results")
    os.makedirs(results_dir, exist_ok=True)
    
    # 1. 詳細ログ（JSON）の保存パス
    log_output_path = os.path.join(results_dir, f"{original_task_id}_log.json")
    # 2. 結合した翻訳テキストの保存パス
    text_output_path = os.path.join(results_dir, f"{original_task_id}_final.txt")

    # チャンクは並列実行されるため、順番がバラバラになる可能性があります。
    # input_path（...chunk_0.txt, chunk_1.txt）をキーにソートします。
    sorted_results = sorted(
        results, 
        key=lambda x: x.get('input_path', '') if isinstance(x, dict) else ''
    )

    combined_text = []
    
    # 子タスクから返ってきた辞書を処理
    for res in sorted_results:
        if isinstance(res, dict) and res.get("status") == "success":
            combined_text.append(res.get("translated_text", ""))
        elif isinstance(res, dict) and res.get("status") in ["error", "aborted"]:
            # エラーがあった箇所の記録
            combined_text.append(f"\n[ERROR/ABORTED at {res.get('path', 'unknown')}]\n")
        else:
            # 予期しない形式の場合のフォールバック
            combined_text.append(str(res))

    # --- 保存処理 ---
    
    # 翻訳テキストのみを結合して書き出し（実用的な成果物）
    with open(text_output_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(combined_text))

    # 実行結果の詳細（メタデータ含む）をJSONとして保存
    with open(log_output_path, 'w', encoding='utf-8') as f:
        json.dump(sorted_results, f, ensure_ascii=False, indent=2)

    return {
        "status": "completed", 
        "final_text_path": text_output_path,
        "log_path": log_output_path,
        "total_chunks": len(results)
    }