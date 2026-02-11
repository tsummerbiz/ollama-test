import os
import shutil
import io
import uuid
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Depends, status
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from jose import JWTError, jwt
#from passlib.context import CryptContext
import bcrypt  # passlib の代わりにこれをインポート

from celery.result import AsyncResult
from celery.contrib.abortable import AbortableAsyncResult

# tasks.py からのインポート
from tasks import app as celery_app, split_and_dispatch, get_task_progress, decrypt_file, get_callback_id

# --- 設定項目 (本番環境では環境変数から取得してください) ---
SECRET_KEY = "YOUR_SUPER_SECRET_KEY_DONT_SHARE" # openssl rand -hex 32 等で生成
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

# ダミーのユーザーデータベース (実際にはDBから取得)
fake_users_db = {
    "admin_user": {
        "username": "admin_user",
        "full_name": "Admin",
        "hashed_password": "$2b$12$WOJtEqRg63Wtm3pEa1K.p.4538RpoXBeqk1o0hHuziHnEPQtcqSPy", # 'password123' のハッシュ
        "disabled": False,
    }
}

#pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

app = FastAPI(title="Distributed Translation System API with JWT")

# --- 静的ファイルの設定 ---
# 'static' ディレクトリを '/test' というURLパスに紐付けます
# html=True にすると、/ にアクセスした際に自動で index.html を探してくれます
app.mount("/test", StaticFiles(directory="static", html=True), name="static")

# ... 既存のCORS設定やエンドポイントのコード ...
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # テスト用。本番はドメインを制限してください
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

NFS_BASE_PATH = Path("/data/")
UPLOAD_DIR = NFS_BASE_PATH / "uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# --- ユーティリティ関数 ---

def verify_password(plain_password: str, hashed_password: str):
    # 文字列をバイト列に変換してチェック
    password_byte = plain_password.encode('utf-8')
    hashed_byte = hashed_password.encode('utf-8')
    return bcrypt.checkpw(password_byte, hashed_byte)

# (参考) 新しくユーザーを作る時のハッシュ化関数
def get_password_hash(password: str):
    pwd_bytes = password.encode('utf-8')
    salt = bcrypt.gensalt()
    hashed_password = bcrypt.hashpw(pwd_bytes, salt)
    return hashed_password.decode('utf-8')

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    
    user = fake_users_db.get(username)
    if user is None:
        raise credentials_exception
    return user

# --- 認証用エンドポイント ---
@app.post("/token", summary="ログインしてアクセストークンを取得")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = fake_users_db.get(form_data.username)
    if not user or not verify_password(form_data.password, user["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(data={"sub": user["username"]})
    return {"access_token": access_token, "token_type": "bearer"}

# --- 保護された既存エンドポイント ---

@app.post("/upload", summary="ファイルをアップロードして翻訳を開始")
async def upload_file(
    file: UploadFile = File(...),
    encryption_password: str = Query(..., description="暗号化用のパスワード"),
    source_lang: str = "Japanese",
    source_code: str = "ja",
    target_lang: str = "English",
    target_code: str = "en",
    chunk_size_kb: int = 4,
    current_user: dict = Depends(get_current_user) # 認証を追加
):
    parent_task_id = str(uuid.uuid4())
    extension = Path(file.filename).suffix
    unique_filename = f"{parent_task_id}{extension}"
    file_path = UPLOAD_DIR / unique_filename
    
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File save error: {e}")

    split_and_dispatch.apply_async(
        kwargs={
            "file_path": str(file_path),
            "encryption_password": encryption_password,
            "chunk_size": chunk_size_kb * 1024,
            "source_lang": source_lang,
            "source_code": source_code,
            "target_lang": target_lang,
            "target_code": target_code
        },
        task_id=parent_task_id
    )
    
    return {
        "message": "Processing started",
        "user": current_user["username"],
        "parent_task_id": parent_task_id,
    }

@app.get("/download/{task_id}", summary="復号化してファイルをダウンロード")
async def download_file(
    task_id: str, 
    password: str = Query(..., description="復号用のパスワード"),
    current_user: dict = Depends(get_current_user) # 認証を追加
):
    callback_id = get_callback_id(task_id)
    task_result = AsyncResult(callback_id, app=celery_app)

    if not task_result.ready():
        raise HTTPException(status_code=400, detail="Task is not finished yet.")
    
    if task_result.status != "SUCCESS":
        raise HTTPException(status_code=400, detail="Task failed or was cancelled.")

    result_data = task_result.result
    file_path_str = result_data.get("final_file") if isinstance(result_data, dict) else None

    if not file_path_str or not os.path.exists(file_path_str):
        raise HTTPException(status_code=404, detail="Result file not found.")

    try:
        decrypted_content = decrypt_file(Path(file_path_str), password)
        filename = f"translated_{task_id}.txt"
        return StreamingResponse(
            io.BytesIO(decrypted_content.encode('utf-8')),
            media_type="text/plain",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Decryption failed. Error: {str(e)}")

@app.get("/status/{task_id}")
async def get_status(task_id: str, current_user: dict = Depends(get_current_user)):
    task_result = AsyncResult(task_id, app=celery_app)
    
    response = {
        "task_id": task_id,
        "celery_status": task_result.status,
        "progress": get_task_progress(task_id),
    }
    
    callback_id = get_callback_id(task_id)
    callback_result = AsyncResult(callback_id, app=celery_app)
   
    if callback_result.ready() and callback_result.status == "SUCCESS":
        response["download_url"] = f"/download/{task_id}?password=..."
        response["result"] = task_result.result

    return JSONResponse(content=response)

@app.post("/cancel/{task_id}", summary="タスクの中断")
async def cancel_task(task_id: str, current_user: dict = Depends(get_current_user)):
    abortable_detail = AbortableAsyncResult(task_id, app=celery_app)
    abortable_detail.abort()
    celery_app.control.revoke(task_id, terminate=True)
    return {"message": f"Cancellation signal sent to task {task_id}", "by": current_user["username"]}