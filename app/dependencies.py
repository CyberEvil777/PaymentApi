import secrets
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def get_settings(request: Request) -> Settings:
    return request.app.state.settings


async def require_api_key(
    key: Annotated[str | None, Security(api_key_header)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> None:
    if key is None or not secrets.compare_digest(key.encode(), settings.api_key.encode()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing API key"
        )


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    """Управляет временем жизни сессии; сервис выполняет commit и rollback."""
    async with request.app.state.session_factory() as session:
        yield session


SessionDependency = Annotated[AsyncSession, Depends(get_session)]
