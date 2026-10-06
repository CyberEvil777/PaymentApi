# Асинхронный процессинг платежей

FastAPI + Pydantic v2, SQLAlchemy 2 async / asyncpg, PostgreSQL, Alembic,
RabbitMQ через FastStream. Один consumer выполняет оплату и webhook;
outbox publisher работает в том же процессе как управляемая задача.
Python 3.12. Точные версии (включая FastStream 0.7.4) — `requirements.lock`.

## Запуск

Нужны Docker Engine и Docker Compose v2.20+ (либо совместимая более новая версия),
доступ к Docker daemon, реестру образов и PyPI. Команды выполняются **в каталоге
с этим README и `.git`** — во вложенном `PaymentApi/` исходного workspace.

### Существующие данные RabbitMQ

До первого запуска обновлённого Compose с существующим volume сохраните прежний
hostname узла. Пока старый контейнер ещё существует (в том числе остановленный):

```bash
docker inspect --format '{{.Config.Hostname}}' "$(docker compose ps -a -q rabbitmq)"
```

Запишите полученное значение в `RABBITMQ_HOSTNAME` своего `.env`. Для нового
окружения оставьте `rabbitmq`. После инициализации volume значение менять нельзя:
[каталог данных RabbitMQ зависит от имени узла](https://www.rabbitmq.com/docs/relocate).
Если старый контейнер уже удалён, сначала установите прежнее имя узла по сохранённому
volume/резервной копии; не запускайте его с новым именем наугад. Автоматического
переноса данных и удаления volumes здесь нет. Сохраните и прежнее имя Compose project:
имена его volumes должны остаться теми же.

### Подготовка и запуск

```bash
# Существующий .env не перезаписывается.
test -f .env || cp .env.example .env
# Задайте свой API_KEY (минимум 16 символов); для существующего RabbitMQ см. выше.
docker compose config --quiet
docker compose up --build -d --wait --wait-timeout 180
docker compose ps -a
docker compose logs --tail=100 migrate api consumer
```

API: http://localhost:8000/docs. RabbitMQ management: http://localhost:15672
(локальные учебные credentials `payments` / `payments`). Все опубликованные порты
привязаны к loopback; их можно изменить через `API_PORT`, `POSTGRES_PORT`,
`AMQP_PORT`, `RABBITMQ_MANAGEMENT_PORT`. Эти параметры меняют только порты хоста.
Внутри сети используются `postgres:5432` и `rabbitmq:5672`. `DATABASE_URL` и
`RABBITMQ_URL` в `.env.example` предназначены для локального Python: Compose явно
задаёт внутренние URL и не использует эти два значения из `.env`.

Образ устанавливает точные версии из `requirements.lock`, затем выполняет `pip check`.
Он содержит только приложение, миграции, тесты и необходимые конфигурации.
Тесты используют тот же образ и lock-файл; `.env`, Git, IDE, документация и локальные
артефакты в него не копируются. Процесс работает с UID 10001, `WORKDIR=/service`;
запись нужна только во временный каталог для heartbeat. Собственный entrypoint не нужен.
Теги базовых образов остаются обновляемыми: это воспроизводимый состав Python-зависимостей,
но не обещание побитово одинаковой сборки на разных датах.

Compose ждёт TCP-готовности PostgreSQL и готовности приложения/слушателей RabbitMQ.
Единственный одноразовый сервис `migrate` выполняет `alembic upgrade head`; API и
consumer ждут успешного завершения. Outbox publisher запускается внутри consumer.
API может принимать платежи при недоступном RabbitMQ: события сохраняются в БД.
Повторный `up` безопасен: Alembic не применяет уже выполненные ревизии повторно.
Не запускайте отдельные процессы миграций параллельно для одной БД.

Для обновления кода и схемы уже запущенного проекта:

```bash
docker compose build
docker compose stop api consumer
docker compose run --rm migrate
docker compose up -d --wait --wait-timeout 180 api consumer
docker compose exec api alembic current
```

Логи, перезапуск consumer и остановка **без удаления данных**:

```bash
docker compose logs -f --tail=100 api consumer
docker compose restart consumer
docker compose stop
# Возобновление с проверкой готовности:
docker compose up -d --wait --wait-timeout 180
```

PostgreSQL и RabbitMQ используют прежние именованные volumes; у них, API и consumer
задана политика `unless-stopped`. Статус `unhealthy` сам по себе не перезапускает
контейнер: restart policy реагирует на остановку процесса.
SIGTERM останавливает publisher, завершает текущие обработчики и закрывает клиенты.
Неподтверждённая доставка возвращается брокеру. `init: true` передаёт сигналы дочернему
процессу, Compose даёт 45 секунд, FastStream — до 25 секунд на завершение обработчика.
Healthcheck API проверяет БД; consumer обновляет heartbeat при доступных БД и RabbitMQ.
При аварии или превышении времени завершения возможна повторная доставка, как описано ниже.

### Проверка обычного окружения в отдельном проекте

```bash
bash scripts/docker-smoke.sh
```

Скрипт требует Bash и Docker, создаёт уникальный Compose project и назначает свободные
порты хоста. Он собирает и запускает обычную конфигурацию, проверяет миграции и healthchecks,
поднимает временный HTTP-приёмник в сети consumer, проверяет фактически полученный webhook,
конечный статус, повторный POST с тем же ключом и единственность payment/исходного outbox.
Затем повторяет `up`, создаёт платёж при остановленном consumer, запускает и перезапускает
consumer и ждёт доставки накопленного события. Шлюз работает без подмены случайности;
оба бизнес-результата допустимы. Все ожидания готовности ограничены таймаутами.
При выходе скрипт останавливает созданные контейнеры и печатает имя проекта;
volumes и контейнеры не удаляются, пользовательское окружение не затрагивается.
Это отдельная проверка Docker-запуска, дополнение к pytest.

### Локально без контейнера приложения

```bash
python3.12 -m venv .venv
. .venv/bin/activate
pip install -r requirements.lock
test -f .env || cp .env.example .env
# PostgreSQL и RabbitMQ должны быть доступны по URL из .env.
set -a; . ./.env; set +a
alembic upgrade head
uvicorn app.api:create_app --factory --host 127.0.0.1 --port 8000
# В другом терминале, с теми же переменными окружения:
python -m app.consumer
```

Alembic читает `DATABASE_URL` из окружения; приложение также умеет читать `.env`.
Для воспроизводимости используйте lock-файл. `pyproject.toml` описывает допустимые
диапазоны зависимостей и конфигурацию инструментов.

## API

Все `/api/v1/*` требуют `X-API-Key`; отсутствующий/неверный ключ → 401.
`/health`, `/docs`, `/openapi.json` служебные и публичные.

```bash
export API_KEY=local-test-api-key-change-me  # используйте значение из своего .env
curl -i http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" \
  -H 'Idempotency-Key: order-42' \
  -H 'Content-Type: application/json' \
  -d '{"amount":"123.45","currency":"RUB","description":"Заказ 42","metadata":{"order_id":42},"webhook_url":"https://receiver.example/payments"}'
```

`receiver.example` — пример: замените его работающим HTTP/HTTPS endpoint.
`localhost` внутри consumer означает сам контейнер, а не хостовую машину.

Ответ 202:

```json
{"payment_id":"e6d29d17-16ca-4162-bbd7-d1e1d9966120","status":"pending","created_at":"2026-10-06T06:00:00Z"}
```

```bash
curl -i http://localhost:8000/api/v1/payments/e6d29d17-16ca-4162-bbd7-d1e1d9966120 \
  -H "X-API-Key: $API_KEY"
```

GET возвращает `payment_id`, `amount` (строка), `currency`, `description`, `metadata`,
`status`, `webhook_url`, `created_at`, `processed_at`. Неизвестный UUID → 404,
некорректный UUID → 422.

Ограничения запроса:

- Все пять полей обязательны. `metadata` — JSON-объект, `description` — до 2000 символов
  (пустая строка допустима). Неизвестные поля запрещены.
- `amount`: `NUMERIC(18,2)`, от `0.01` до `9999999999999999.99`, максимум два
  значащих десятичных разряда после точки. Лишние конечные нули допустимы.
  Передавайте десятичную **строку** или целое JSON-число. JSON float запрещён,
  округления нет. В Python используется `Decimal`; валюты только RUB/USD/EUR.
- `webhook_url`: HTTP/HTTPS, до 2048 символов. Redirect не является успехом.
- `Idempotency-Key`: обязательный, 1–200 символов, глобально уникальный.

Повтор ключа с тем же нормализованным содержимым возвращает тот же ID и текущий
статус с 202, без нового события. Другой body → 409. Суммы `12.3` и `12.30`
равны; URL нормализуется Pydantic; порядок ключей JSON не важен. Типы значений
metadata различаются (например, `true` и `1` — разные запросы).

## Структура API

- `app/api.py` — фабрика приложения, lifecycle, подключение роутера и безопасный
  ответ на ошибки валидации.
- `app/routes/payments.py` — HTTP-контракт POST/GET, преобразование ожидаемого
  конфликта в 409 и явные DTO ответов.
- `app/dependencies.py` — настройки, проверка API-ключа и отдельная сессия на запрос.
  Зависимость закрывает сессию, но не делает commit.
- `app/payments.py` — сценарий создания, SQL и единственная транзакция payment + outbox.
  Сервис ожидает сессию без активной транзакции и возвращается только после commit.
  `ON CONFLICT` и уникальное ограничение обеспечивают конкурентную идемпотентность.

Repository/unit of work не используются. Создание сессии не означает открытия
соединения с БД; валидация запроса не требует SQL. Настройки и фабрику сессий можно
подменить в `create_app`, отдельные зависимости — через `dependency_overrides`.
OpenAPI описывает API-key security scheme и сумму как строку или целое число.
Невалидные `NaN`/`Infinity` и переполнение JSON-числа теперь возвращают 422:
нефинитные значения внутри описания ошибки заменяются строками. Остальная форма
ошибок валидации и существующие успешные ответы сохранены.

## Транзакции и доставка

```text
POST → [PostgreSQL: payment + outbox, один COMMIT]
     → publisher → RabbitMQ payments.new → один consumer
     → [COMMIT результата шлюза] → HTTP webhook → [COMMIT доставки]
                                          ↓ техническая ошибка
                         [COMMIT счётчика + отложенного outbox / DLQ]
```

Внешнее `metadata` хранится в колонке `metadata`; ORM-атрибут — `payment_metadata`,
чтобы не конфликтовать с `DeclarativeBase.metadata`.

`INSERT ... ON CONFLICT DO NOTHING` использует уникальность `idempotency_key`
и ждёт конкурирующую транзакцию. Последующий SELECT при PostgreSQL READ COMMITTED
видит её результат. Payment и исходный outbox создаются одной транзакцией:
ошибка вставки события откатывает и платёж.

Publisher выбирает только `published_at IS NULL AND available_at <= now`,
блокирует строку через `FOR UPDATE SKIP LOCKED` и публикует persistent-сообщение
с `mandatory=True`, publisher confirms, `on_return_raises=True` и таймаутом 10 с.
Только положительный AMQP `Basic.Ack` разрешает установить `published_at`.
Ошибка/timeout/отсутствие маршрута сохраняют событие; отправка повторяется
с backoff 2, 4, 8, 16, 30 секунд, далее по 30 секунд. Этот транспортный счётчик
отделён от трёх попыток обработки платежа. Все состояния переживают перезапуск.

Сбой после broker confirm до COMMIT может повторно опубликовать событие с тем же
AMQP message ID. Consumer блокирует строку payment, проверяет сохранённые
`completed_attempts` и `webhook_delivered_at`. Устаревшие/завершённые доставки
подтверждаются без повторной работы. Для каждой логической попытки существует
уникальный `outbox.event_key`.

Шлюз: `await` задержки 2–5 секунд, затем 90% `succeeded`, 10% `failed`.
Оба результата финальны, имеют `processed_at` с часовым поясом и требуют webhook.
Результат коммитится **до** HTTP. Ошибка webhook его не изменяет и не повторяет
оплату. `httpx.AsyncClient` использует сетевые таймауты, отключённые redirects и проверку
статуса (успех только 2xx). `WEBHOOK_TIMEOUT_SECONDS` также ограничивает полное
время доставки через `asyncio.timeout`. Ответ читается в streaming-режиме:
после проверки статуса ненужное тело не загружается. Это исключает зависание
на бесконечном потоке тела и его неограниченную буферизацию.

## Retry, exchanges и DLQ

| Назначение | Exchange | Routing key / очередь | Хранение |
|---|---|---|---|
| Новая и повторная попытки | стандартный direct exchange `""` | `payments.new` | durable очередь, persistent сообщения |
| Исчерпанные попытки | стандартный direct exchange `""` | `payments.dlq` | durable очередь, persistent сообщения |
| Ожидание повтора | PostgreSQL outbox | `available_at`, payload `attempt` | транзакционно, без отдельной Rabbit retry-очереди |

**Три логические попытки всего**, включая первую. После первой ошибки следующая
доступна не ранее чем через 2 с, после второй — через 4 с. После третьей создаётся
DLQ-событие. В DLQ: payment ID, `attempts=3`, тип ошибки/HTTP-код, результат оплаты,
стабильный ID webhook и время ошибки. Тела ответов и URL с credentials не сохраняются
в причине ошибки.

Вместо TTL + DLX выбран **отложенный transactional outbox**: тот же механизм,
который уже нужен для создания платежа, атомарно хранит счётчик и будущую доставку.
Физические retry-очереди, TTL и пользовательские exchanges/DLX не используются.
Это также исключает окно потери при переносе между очередями через обычный DLX.
Задержка — минимальная: остановленный consumer/publisher или outage увеличит её.

При ошибке обработки consumer коммитит счётчик + retry/DLQ outbox одной транзакцией
и затем ack исходного сообщения: обязательство повторной отправки уже надёжно
сохранено в БД. Publisher подтвердит доставку retry/DLQ отдельно. Если БД недоступна
и зафиксировать состояние нельзя, сообщение остаётся **unacked**, обработчик ждёт
5 с и пробует восстановить работу; нет `nack(requeue=True)` и busy loop.
Повторяются только распознанные временные ошибки соединения/транзакции.
Неожиданная или постоянная ошибка (например, несовместимая схема БД) останавливает
worker с ненулевым кодом, сохраняя сообщение unacked; Compose перезапустит его,
но первопричину необходимо устранить. Ошибка ack не запускает обработку заново:
доставка закрытого канала оставляется брокеру для redelivery, при прочих ошибках
ack worker закрывается. Фоновые задачи завершаются и при аварии.
Outage хранилища/брокера не расходует логические попытки шлюза/webhook.

В FastStream 0.7.4 явно задан `AckPolicy.MANUAL`: framework не выполняет автоматический
ack/nack, вызов `message.ack()` только после COMMIT либо подтверждённой публикации
некорректного внешнего события в DLQ. Проверена реализация installed package;
описание политики: [FastStream acknowledgement](https://faststream.ag2.ai/latest/getting-started/acknowledgement/).
Publisher confirms: [Channel](https://faststream.ag2.ai/latest/api/faststream/rabbit/Channel/).
Некорректные внутренние события/неизвестный payment ID сразу помещаются в DLQ
с `error=InvalidPaymentEvent`, `attempts=0` и исходными байтами в `original_base64`.
Декодирование и проверка JSON выполняются внутри ручного обработчика, поэтому
массив, строка, невалидный JSON и невалидный UTF-8 не обходят решение об ack/DLQ.
Исходное сообщение подтверждается после broker confirm. Штатный API таких
событий не создаёт; формат DLQ для исчерпанных попыток платежа не изменён.

Посмотреть DLQ можно в management UI → Queues → `payments.dlq` → Get messages.
Автоматического возврата из DLQ и удаления истории outbox нет.

## Webhook: at-least-once

```json
{
  "event_id":"98413c5d-1fbe-40ce-9e62-6b9d1d8b191f",
  "payment_id":"e6d29d17-16ca-4162-bbd7-d1e1d9966120",
  "status":"succeeded",
  "processed_at":"2026-10-06T06:00:04Z"
}
```

`event_id` создаётся один раз вместе с платежом и передаётся также в заголовке
`Idempotency-Key`. Получатель должен атомарно дедуплицировать его у себя.
Сбой после успешного HTTP, но до COMMIT доставки может повторить HTTP-запрос;
exactly-once не обещается. Сбой до фиксации завершения попытки может повторить
ту же логическую попытку; лимит 3 относится к сохранённым исходам, а не к числу
TCP-запросов при аварийных остановках. Уже сохранённый результат оплаты не вычисляется
заново. Для настоящего внешнего шлюза дополнительно потребовался бы его собственный
idempotency key (здесь шлюз — эмулятор без внешнего списания денег).

## Проверки

Результаты, подтверждённые пользователем на его Docker-окружении:

- Полный интеграционный прогон: **107 passed**.
- После исправлений пользователь сообщил об **успешном завершении Docker smoke**.

Это результаты пользовательских запусков, а не повторный прогон ассистента.
Наличие или отсутствие warnings не подтверждено.

Все команды выполняются из корня Git-репозитория. Полный набор на настоящих
PostgreSQL и RabbitMQ:

```bash
docker compose -f docker-compose.test.yml up --build \
  --abort-on-container-exit --exit-code-from tests
docker compose -f docker-compose.test.yml stop
```

Конфигурация использует отдельный project `payment-processor-tests`, не публикует
порты и не подключает volumes основного окружения. Для отдельного нового прогона
добавьте `-p payment-tests-ИМЯ` к обеим командам. Тесты очищают свои очереди;
не запускайте их параллельно в одном project или на рабочих БД/vhost.
Без `TEST_DATABASE_URL`/`TEST_RABBITMQ_URL` локальный pytest пропускает соответствующие
интеграционные сценарии; тестовый Compose задаёт эти переменные автоматически.

Проверка обычного Docker-запуска с реальным HTTP-приёмником, повторным POST,
повторным запуском окружения и восстановлением consumer:

```bash
bash scripts/docker-smoke.sh
```

Скрипт сам выбирает уникальный project и свободные порты, затем останавливает
созданные контейнеры без удаления данных. Подробнее — в разделе запуска выше.

Быстрые локальные проверки после установки `requirements.lock`:

```bash
python -m pytest -q -m 'not integration'
ruff check .
ruff format --check .
docker compose --env-file .env.example config --quiet
docker compose -f docker-compose.test.yml config --quiet
```

Тесты проверяют API, конкурентную идемпотентность, атомарность payment + outbox,
миграции, восстановление publisher, повторные доставки, оба результата шлюза,
webhook, retry/DLQ и разрыв соединения перед ack. Время и случайность подменяются
через зависимости; транспортные интеграционные проверки используют реальные сервисы.

## Допущения и границы

- Один consumer, prefetch=1. Блокировка payment держится во время эмуляции/HTTP;
  это простое решение для тестового, не расчёт на высокую пропускную способность.
- Статический API-ключ означает доверенных клиентов. Разрешены любые HTTP/HTTPS URL,
  включая внутренние адреса для локальной проверки; для публичного production API
  нужна отдельная политика исходящего доступа/SSRF и аутентификация webhook.
- Технический отказ шлюза на всех попытках оставляет payment `pending` в DLQ:
  бизнес-результат оплаты неизвестен. Отказ webhook оставляет сохранённый
  `succeeded`/`failed`. Дополнительные бизнес-статусы не вводятся.
- PostgreSQL и RabbitMQ в Compose — по одному узлу с volumes, без HA.
  Надёжность предполагает сохранность их дисков; удаление volumes — потеря данных.
- История outbox и idempotency keys не очищается автоматически; ключи бессрочны.
  Максимум metadata/request body отдельно не ограничен; для внешнего развёртывания
  лимит тела обычно задаётся на reverse proxy.
