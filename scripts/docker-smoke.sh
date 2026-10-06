#!/usr/bin/env bash
# Проверка настоящего окружения в отдельном проекте; volumes не удаляются.
set -euo pipefail
cd "$(dirname "$0")/.."
export API_KEY=smoke-only-api-key-32-characters
export API_PORT=0 POSTGRES_PORT=0 AMQP_PORT=0 RABBITMQ_MANAGEMENT_PORT=0
export RABBITMQ_HOSTNAME=rabbitmq
SMOKE_PROJECT="payment-smoke-$(date +%s)-${RANDOM}"
SMOKE_RECEIVER="${SMOKE_PROJECT}-receiver"
dc() { docker compose --env-file .env.example -p "$SMOKE_PROJECT" -f docker-compose.yml "$@"; }
cleanup() {
    local result=$?
    trap - EXIT
    dc logs --no-color --tail=40 migrate api consumer || true
    docker stop --time 10 "$SMOKE_RECEIVER" >/dev/null 2>&1 || true
    dc stop || true
    echo "Проект $SMOKE_PROJECT остановлен; контейнеры и volumes сохранены."
    exit "$result"
}
docker info >/dev/null
trap cleanup EXIT
printf 'Проверяем изолированный проект %s\n' "$SMOKE_PROJECT"
dc up --build -d --wait --wait-timeout 180
dc ps -a
dc exec -T api alembic current
# Приёмник находится в той же сети и не публикует порт на хосте.
dc run -d --no-deps --name "$SMOKE_RECEIVER" api python -u -c '
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
received = []
class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        received.append({"body": body, "key": self.headers.get("Idempotency-Key")})
        print(json.dumps(received[-1]), flush=True)
        self.send_response(204)
        self.end_headers()
    def do_GET(self):
        body = json.dumps(received).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
HTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
'
probe() {
    dc exec -T -e SMOKE_RECEIVER="$SMOKE_RECEIVER" api python - "$@" <<'PY'
import asyncio
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from sqlalchemy import text
from app.config import Settings
from app.db import database

mode, key = sys.argv[1:]
base = "http://127.0.0.1:8000/api/v1/payments"
receiver = "http://" + os.environ["SMOKE_RECEIVER"] + ":8000/"
headers = {"X-API-Key": os.environ["API_KEY"], "Idempotency-Key": key,
           "Content-Type": "application/json"}
body = {"amount": "12.34", "currency": "RUB", "description": "Docker smoke",
        "metadata": {}, "webhook_url": receiver}
saved = Path("/tmp/" + key + ".json")

def request(url, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=headers), timeout=3) as r:
        return r.status, json.load(r)

if mode == "create":
    deadline = time.monotonic() + 20
    while True:
        try:
            request(receiver)
            break
        except (urllib.error.URLError, TimeoutError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.2)
    code, payment = request(base, body)
    assert code == 202, (code, payment)
    saved.write_text(json.dumps(payment))
    print("Создан платёж:", payment)
else:
    payment = json.loads(saved.read_text())
    deadline = time.monotonic() + 30
    while True:
        _, detail = request(base + "/" + payment["payment_id"])
        _, events = request(receiver)
        matches = [e for e in events if e["body"]["payment_id"] == payment["payment_id"]]
        if detail["status"] in ("succeeded", "failed") and matches:
            break
        assert time.monotonic() < deadline, (detail, events)
        time.sleep(0.2)
    assert all(e["body"]["status"] == detail["status"] for e in matches)
    assert len({e["body"]["event_id"] for e in matches}) == 1
    assert all(e["key"] == e["body"]["event_id"] for e in matches)
    code, repeated = request(base, body)
    assert code == 202 and repeated["payment_id"] == payment["payment_id"]

    async def check_rows():
        engine, sessions = database(Settings().database_url)
        try:
            async with sessions() as session:
                count = await session.scalar(text(
                    "SELECT count(*) FROM payments WHERE idempotency_key = :key"), {"key": key})
                initial_events = await session.scalar(text(
                    "SELECT count(*) FROM outbox o JOIN payments p ON p.id = o.payment_id "
                    "WHERE p.idempotency_key = :key AND o.event_key = CAST(p.id AS text) || :event_suffix"),
                    {"key": key, "event_suffix": ":1"})
                assert (count, initial_events) == (1, 1), (count, initial_events)
        finally:
            await engine.dispose()
    asyncio.run(check_rows())
    print("Подтверждены результат, получение webhook и идемпотентность:", detail)
PY
}
probe create docker-smoke-one
probe check docker-smoke-one
# Повторный up должен успешно пройти одноразовый сервис миграций.
dc up -d --wait --wait-timeout 180
probe check docker-smoke-one
# Платёж создаётся при остановленном publisher; после старта должен дойти до webhook.
dc stop consumer
probe create docker-smoke-two
dc start consumer
dc restart consumer
dc up -d --wait --wait-timeout 180
probe check docker-smoke-two
docker logs "$SMOKE_RECEIVER"
echo 'Docker smoke: все проверки выполнены успешно.'
