import math
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.db import database
from app.dependencies import SessionDependency
from app.routes.payments import router


def finite_error_values(value: object) -> object:
    """Сохраняет формат ошибок валидации, заменяя только недопустимые для JSON числа."""
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, dict):
        return {key: finite_error_values(item) for key, item in value.items()}
    if isinstance(value, list):
        return [finite_error_values(item) for item in value]
    return value


async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content={"detail": finite_error_values(jsonable_encoder(exc.errors()))},
    )


def create_app(
    settings: Settings | None = None,
    sessions: async_sessionmaker[AsyncSession] | None = None,
) -> FastAPI:
    settings = settings or Settings()
    engine = None
    if sessions is None:
        engine, sessions = database(settings.database_url)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            if engine is not None:
                await engine.dispose()

    app = FastAPI(title="Async Payment Processor", lifespan=lifespan)
    app.state.settings = settings
    app.state.session_factory = sessions
    app.add_exception_handler(RequestValidationError, validation_error_handler)
    app.include_router(router)

    @app.get("/health", include_in_schema=False)
    async def health(session: SessionDependency) -> dict[str, str]:
        await session.execute(text("SELECT 1"))
        return {"status": "ok"}

    return app
