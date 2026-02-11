import os
import time
import requests
from typing import List, Optional
from jose import jwt, JWTError
from fastapi import HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

# 環境変数の読み込み
JWKS_URL = os.getenv("JWKS_URL", "http://host.docker.internal/jwtauth")
JWT_ISSUER = os.getenv("JWT_ISSUER", "JwtAuthApi")
JWT_AUDIENCE = os.getenv("JWT_AUDIENCE", "Any")
JWKS_REFRESH_COOLDOWN = int(os.getenv("JWKS_REFRESH_COOLDOWN", "60"))

security = HTTPBearer()

class JwksClient:
    """JWKSの取得とキャッシュ管理を行うクラス"""
    def __init__(self):
        self.cached_keys = {}
        self.last_refresh_time = 0

    def _fetch_keys(self):
        """JWKSエンドポイントから公開鍵を取得"""
        try:
            response = requests.get(JWKS_URL, timeout=10)
            response.raise_for_status()
            jwks = response.json()
            self.cached_keys = {key['kid']: key for key in jwks.get('keys', [])}
            self.last_refresh_time = time.time()
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Unable to fetch JWKS: {str(e)}"
            )

    def get_key(self, kid: str):
        """指定されたkidに対応する公開鍵を返す。存在しない場合はクールタイム後に再取得。"""
        if kid not in self.cached_keys:
            current_time = time.time()
            if current_time - self.last_refresh_time > JWKS_REFRESH_COOLDOWN:
                self._fetch_keys()
            
        key = self.cached_keys.get(kid)
        if not key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid key ID (kid) or public key not found."
            )
        return key

jwks_client = JwksClient()

class JWTChecker:
    """JWTの検証および認可を行う依存注入用クラス"""
    def __init__(self, allowed_roles: List[str] = None, required_app_code: str = None):
        self.allowed_roles = allowed_roles
        self.required_app_code = required_app_code

    async def __call__(self, creds: HTTPAuthorizationCredentials = Depends(security)):
        token = creds.credentials
        
        try:
            # ヘッダーからkidを取得
            unverified_header = jwt.get_unverified_header(token)
            kid = unverified_header.get("kid")
            if not kid:
                raise HTTPException(status_code=401, detail="Missing kid in token header")

            # 対応する公開鍵を取得
            public_key = jwks_client.get_key(kid)

            # トークンの検証
            payload = jwt.decode(
                token,
                public_key,
                algorithms=["RS256"],
                audience=JWT_AUDIENCE,
                issuer=JWT_ISSUER
            )

            # 認可ロジック: app_code のチェック
            if self.required_app_code:
                if payload.get("app_code") != self.required_app_code:
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN, 
                        detail="Invalid Application Code"
                    )

            # 認可ロジック: role のチェック
            if self.allowed_roles:
                user_role = payload.get("role")
                if user_role not in self.allowed_roles:
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN, 
                        detail="Insufficient permissions"
                    )

            return payload

        except JWTError as e:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Token validation failed: {str(e)}"
            )