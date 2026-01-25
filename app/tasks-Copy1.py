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

def _save_chunk(task_id, idx, lines):
    """チャンクをファイルに保存するヘルパー関数"""
    chunk_filename = f"{task_id}_chunk_{idx}.txt"
    chunk_path = os.path.join(NFS_BASE_PATH, "temp", chunk_filename)
    with open(chunk_path, 'w', encoding='utf-8') as cf:
        cf.writelines(lines)
    return chunk_path    

@app.task(bind=True)
def split_and_dispatch(self, file_path: str, chunk_size: int = 1024 * 100):  # デフォルトを100KBなどに設定
    """
    【親タスク】ファイルを1行ずつ読み込み、バイト数ベースでチャンク分割して子タスクを発行する
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    task_id = self.request.id
    chunk_paths = []
    
    os.makedirs(os.path.join(NFS_BASE_PATH, "temp"), exist_ok=True)

    with open(file_path, 'r', encoding='utf-8') as f:
        chunk_idx = 0
        current_chunk_lines = []
        current_chunk_bytes = 0

        for line in f:
            line_bytes = len(line.encode('utf-8'))

            # もし1行でチャンクサイズを超える場合のハンドリング（任意）
            # ここでは「最低1行は保持し、超えたら次のチャンクへ」という挙動にします
            if current_chunk_bytes + line_bytes > chunk_size and current_chunk_lines:
                # 現在のチャンクを書き出し
                chunk_path = _save_chunk(task_id, chunk_idx, current_chunk_lines)
                chunk_paths.append(chunk_path)
                
                # リセット
                chunk_idx += 1
                current_chunk_lines = []
                current_chunk_bytes = 0

            current_chunk_lines.append(line)
            current_chunk_bytes += line_bytes

        # 残りのデータを書き出し
        if current_chunk_lines:
            chunk_path = _save_chunk(task_id, chunk_idx, current_chunk_lines)
            chunk_paths.append(chunk_path)

    # Chordの構築
    callback = aggregate_results.s(original_task_id=task_id)
    header = [process_chunk.s(path) for path in chunk_paths]
    
    chord(header)(callback)
    
    return {"status": "dispatched", "chunks": len(chunk_paths)}

@app.task(bind=True, base=AbortableTask)
def process_chunk(self, chunk_path: str):
    """
    【子タスク】Ollamaを使用して推論を行う。中断可能(Abortable)。
    """
    # 処理開始前のキャンセルチェック
    if self.is_aborted():
        return {"status": "aborted", "path": chunk_path}

    results = []
    
    try:
        with open(chunk_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        for line in lines:
            # ループごとのキャンセルチェック（重要）
            if self.is_aborted():
                return {"status": "aborted", "processed_count": len(results)}

            prompt = line.strip()
            if not prompt:
                continue

            # Ollamaへのリクエスト
            try:
                payload = {
                    "model": "translategemma:12b", # 使用するモデル名に合わせて変更
                    "prompt": prompt,
                    "stream": False
                }
                response = requests.post(
                    OLLAMA_API_URL, 
                    json=payload, 
                    timeout=60 # タイムアウト設定
                )
                response.raise_for_status()
                data = response.json()
                results.append({
                    "input": prompt,
                    "output": data.get("response", "")
                })
            except requests.RequestException as e:
                results.append({"input": prompt, "error": str(e)})

    except Exception as e:
        return {"status": "error", "message": str(e)}
    
    # 処理が終わったチャンクファイルは削除してもよいが、デバッグ用に残すか要件次第
    # os.remove(chunk_path) 
    
    return results

@app.task
def aggregate_results(results, original_task_id: str):
    """
    【集約タスク】子タスクの結果を結合してNFSに保存する
    """
    final_output_path = os.path.join(NFS_BASE_PATH, "results", f"{original_task_id}_result.json")
    os.makedirs(os.path.dirname(final_output_path), exist_ok=True)
    
    # フラットなリストに変換
    flat_results = []
    for res in results:
        if isinstance(res, list):
            flat_results.extend(res)
        else:
            flat_results.append(res) # エラーや中断時のメッセージなど

    with open(final_output_path, 'w', encoding='utf-8') as f:
        json.dump(flat_results, f, ensure_ascii=False, indent=2)

    return {"status": "completed", "output_path": final_output_path}