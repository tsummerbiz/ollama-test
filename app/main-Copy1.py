import os
import shutil
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import JSONResponse
from celery.result import AsyncResult
from celery.contrib.abortable import AbortableAsyncResult

# tasks.py から必要な関数とインスタンスをインポート
from tasks import app as celery_app, split_and_dispatch, get_task_progress

app = FastAPI(title="Distributed Translation System API")

# tasks.py と合わせたパス設定
NFS_BASE_PATH = Path("/data/")
UPLOAD_DIR = NFS_BASE_PATH / "uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

@app.post("/upload", summary="ファイルをアップロードして翻訳を開始")
async def upload_file(
    file: UploadFile = File(...),
    source_lang: str = "Japanese",
    source_code: str = "ja",
    target_lang: str = "English",
    target_code: str = "en",
    chunk_size_kb: int = 50
):
    """
    ファイルをアップロードし、分散処理タスクを発行します。
    - **chunk_size_kb**: 分割サイズ（デフォルト50KB）
    """
    file_path = UPLOAD_DIR / file.filename
    
    # 1. ファイルをNFSに保存
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File save error: {e}")

    # 2. 親タスク (split_and_dispatch) を呼び出し
    # 言語設定などの引数を渡す
    task = split_and_dispatch.delay(
        file_path=str(file_path),
        chunk_size=chunk_size_kb * 1024,
        source_lang=source_lang,
        source_code=source_code,
        target_lang=target_lang,
        target_code=target_code
    )
    
    return {
        "message": "Processing started",
        "parent_task_id": task.id,
        "file_name": file.filename,
        "config": {
            "source": source_lang,
            "target": target_lang,
            "chunk_size": f"{chunk_size_kb}KB"
        }
    }

@app.get("/status/{task_id}", summary="タスクの進捗状況を確認")
async def get_status(task_id: str):
    """
    Celeryのステータスと、Redisに保存された詳細な進捗率を組み合わせて返します。
    """
    # Celery標準のタスク状態
    task_result = AsyncResult(task_id, app=celery_app)
    
    # Redisから詳細な進捗（完了数/総数）を取得
    progress_detail = get_task_progress(task_id)
    
    response = {
        "task_id": task_id,
        "celery_status": task_result.status,  # PENDING, PROGRESS, SUCCESS, FAILURE
        "progress": progress_detail,         # total, completed, percent
    }

    # タスク完了時の処理
    if task_result.ready():
        if task_result.status == "SUCCESS":
            response["result"] = task_result.result
        else:
            # 失敗時のエラー内容など
            response["error"] = str(task_result.result)

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