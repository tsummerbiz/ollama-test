import os
import shutil
import io
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse
from celery.result import AsyncResult
from celery.contrib.abortable import AbortableAsyncResult


# tasks.py から必要な関数をインポート
# ※ decrypt_file が tasks.py に定義されている前提です
from tasks import app as celery_app, split_and_dispatch, get_task_progress, decrypt_file, get_callback_id

app = FastAPI(title="Distributed Translation System API")

NFS_BASE_PATH = Path("/data/")
UPLOAD_DIR = NFS_BASE_PATH / "uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

@app.post("/upload", summary="ファイルをアップロードして翻訳を開始")
async def upload_file(
    file: UploadFile = File(...),
    encryption_password: str = Query(..., description="暗号化用のパスワード"), # 追加
    source_lang: str = "Japanese",
    source_code: str = "ja",
    target_lang: str = "English",
    target_code: str = "en",
    chunk_size_kb: int = 4
):
    file_path = UPLOAD_DIR / file.filename
    
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File save error: {e}")

    # 親タスクに暗号化パスワードを渡して実行
    task = split_and_dispatch.delay(
        file_path=str(file_path),
        encryption_password=encryption_password, # 追加
        chunk_size=chunk_size_kb * 1024,
        source_lang=source_lang,
        source_code=source_code,
        target_lang=target_lang,
        target_code=target_code
    )
    
    return {
        "message": "Processing started",
        "parent_task_id": task.id,
        "file_name": file.filename
    }

@app.get("/download/{task_id}", summary="復号化してファイルをダウンロード")
async def download_file(
    task_id: str, 
    password: str = Query(..., description="復号用のパスワード")
):
    """
    完了したタスクの成果物を復号し、ストリームとして返却します。
    """
    # 1. RedisからコールバックタスクのIDを取得
    callback_id = get_callback_id(task_id)
    task_result = AsyncResult(callback_id, app=celery_app)

    # 2. タスク完了確認
    if not task_result.ready():
        raise HTTPException(status_code=400, detail="Task is not finished yet.")
    
    if task_result.status != "SUCCESS":
        raise HTTPException(status_code=400, detail="Task failed or was cancelled.")

    # 3. 結果の取得とファイルチェック
    result_data = task_result.result
    # aggregate_results が返す辞書からパスを取得
    file_path_str = result_data.get("final_file") if isinstance(result_data, dict) else None

    if not file_path_str or not os.path.exists(file_path_str):
        raise HTTPException(status_code=404, detail="Result file not found.")

    try:
        # 4. 復号
        decrypted_content = decrypt_file(Path(file_path_str), password)
        
        # 5. ストリーム返却
        # ファイル名を指定してダウンロードさせる
        filename = f"translated_{task_id}.txt"
        return StreamingResponse(
            io.BytesIO(decrypted_content.encode('utf-8')),
            media_type="text/plain",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        # パスワード間違いや復号エラーのハンドリング
        raise HTTPException(status_code=401, detail=f"Decryption failed. Check your password. Error: {str(e)}")

@app.get("/status/{task_id}")
async def get_status(task_id: str):
    task_result = AsyncResult(task_id, app=celery_app)
    
    response = {
        "task_id": task_id,
        "celery_status": task_result.status,
        "progress": get_task_progress(task_id),
    }
    
    # 1. RedisからコールバックタスクのIDを取得
    callback_id = get_callback_id(task_id)
    callback_result = AsyncResult(callback_id, app=celery_app)
   
    if callback_result.ready() and callback_result.status == "SUCCESS":
        # ダウンロードには task_id を使うよう案内
        response["download_url"] = f"/download/{task_id}?password=..."
        response["result"] = task_result.result

    return JSONResponse(content=response)

@app.post("/cancel/{task_id}", summary="タスクの中断")
async def cancel_task(task_id: str):
    """
    指定されたタスクに中断フラグを立て、キュー内の処理を取り消します。
    """
    # 実行中のタスク内の is_aborted() フラグをTrueにする
    abortable_detail = AbortableAsyncResult(task_id, app=celery_app)
    abortable_detail.abort()
    
    # まだ実行されていないタスクをキューから削除
    celery_app.control.revoke(task_id, terminate=True)
    
    return {"message": f"Cancellation signal sent to task {task_id}"}