FROM python:3.12-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /service
COPY requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock && pip check
COPY alembic.ini pyproject.toml ./
COPY app ./app
COPY migrations ./migrations
COPY tests ./tests
RUN useradd --uid 10001 --create-home payment && chmod -R a+rX /service
USER payment
CMD ["uvicorn", "app.api:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
