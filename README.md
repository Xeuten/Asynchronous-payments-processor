# Async Payments Processor

Микросервис асинхронной обработки платежей: принимает запрос на оплату, обрабатывает его через эмулятор платёжного шлюза и уведомляет клиента о результате через webhook.

**Стек:** FastAPI, Pydantic v2, SQLAlchemy 2.0 (async), PostgreSQL, RabbitMQ (FastStream), Alembic, Docker Compose.

## Архитектура

```
 клиент
   │ POST /api/v1/payments  (X-API-Key, Idempotency-Key)
   ▼
┌──────────────────────── api ────────────────────────┐
│ FastAPI ──► одна транзакция: INSERT payments        │
│                              INSERT outbox          │
│ Outbox relay (фоновая задача) ──┐                   │
└─────────────────────────────────┼───────────────────┘
                                  │ SELECT … FOR UPDATE SKIP LOCKED,
                                  │ publish + publisher confirms,
                                  │ published_at = now()
                                  ▼
          exchange "payments" (direct) ──payments.new──► queue "payments.new"
                                                              │
┌──────────────────────── consumer ───────────────────────────┼─┐
│ 1. если status = pending → эмуляция шлюза (2–5 с, 90 % успех) │
│ 2. UPDATE payments SET status … WHERE status = 'pending'      │
│ 3. webhook на webhook_url: 3 попытки, задержка 1 с → 2 с      │
│ 4. успех → ack;  неудача → nack(requeue=False)                │
└───────────────────────────────────────────────────────────┬───┘
                                                            │ dead-lettering
          exchange "payments.dlx" ──payments.new──► queue "payments.new.dlq"
```

### Гарантии доставки

| Механизм | Как реализован |
|---|---|
| **Outbox pattern** | Платёж и событие пишутся в БД одной транзакцией (`app/services/payment_service.py`). Relay (`app/outbox/relay.py`) публикует события и помечает их `published_at` только после подтверждения брокера. Если RabbitMQ недоступен, события ждут в таблице `outbox` и уходят после восстановления связи. |
| **At-least-once и идемпотентный consumer** | Relay может отправить событие повторно (сбой между publish и commit). Consumer списывает деньги только для платежа в статусе `pending`, а UPDATE с условием `WHERE status = 'pending'` защищает от гонок (`app/consumer/processor.py`). |
| **Idempotency-Key** | Уникальный индекс на `payments.idempotency_key`. Повтор с тем же ключом и телом возвращает уже созданный платёж (202), тот же ключ с другим телом даёт 409. Гонка параллельных запросов обрабатывается через `IntegrityError`. |
| **Retry** | 3 попытки отправки webhook с экспоненциальной задержкой `base * 2^(n-1)` (`app/consumer/webhook.py`). Ошибкой считаются сетевая ошибка, таймаут и ответ не 2xx. |
| **Dead Letter Queue** | У `payments.new` задан `x-dead-letter-exchange=payments.dlx`. После 3 неудачных попыток (или при неожиданной ошибке обработки) consumer делает `nack(requeue=False)`, и RabbitMQ перекладывает сообщение в `payments.new.dlq`. |
| **Ручной ack** | Если consumer упадёт во время обработки, сообщение останется в очереди и будет обработано после перезапуска. |

## Запуск

Требуется Docker с Compose v2.

```bash
docker compose up --build
```

Поднимаются сервисы:

| Сервис | Адрес | Назначение |
|---|---|---|
| `api` | http://localhost:8000/docs | API (Swagger UI) + outbox relay; при старте применяет миграции |
| `consumer` | — | обработчик очереди `payments.new` |
| `postgres` | localhost:5432 | БД (`payments`/`payments`) |
| `rabbitmq` | http://localhost:15672 | панель управления (`guest`/`guest`) |
| `webhook-mock` | http://localhost:9000 | тестовый приёмник webhook'ов |

API-ключ по умолчанию: `super-secret-api-key`. Его можно переопределить через переменную `API_KEY` (см. `.env.example`).

## Примеры

### Создание платежа

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: super-secret-api-key" \
  -H "Idempotency-Key: order-1" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "100.50",
        "currency": "RUB",
        "description": "Order #1",
        "metadata": {"order_id": 1},
        "webhook_url": "http://webhook-mock:9000/webhook"
      }'
```

```json
HTTP 202
{"payment_id": "d4081e63-…", "status": "pending", "created_at": "2026-10-11T15:21:30.194397Z"}
```

Повторный запрос с тем же `Idempotency-Key` вернёт тот же `payment_id`.

### Получение платежа

```bash
curl http://localhost:8000/api/v1/payments/<payment_id> \
  -H "X-API-Key: super-secret-api-key"
```

```json
{
  "payment_id": "d4081e63-…",
  "amount": "100.50",
  "currency": "RUB",
  "description": "Order #1",
  "metadata": {"order_id": 1},
  "status": "succeeded",
  "idempotency_key": "order-1",
  "webhook_url": "http://webhook-mock:9000/webhook",
  "created_at": "2026-10-11T15:21:30.194397Z",
  "processed_at": "2026-10-11T15:21:34.614767Z"
}
```

### Webhook

Через 2–5 секунд на `webhook_url` приходит POST:

```json
{"event": "payment.succeeded", "payment_id": "d4081e63-…", "status": "succeeded",
 "amount": "100.50", "currency": "RUB", "processed_at": "2026-10-11T15:21:34.614767Z"}
```

Полученные уведомления можно посмотреть так: `curl http://localhost:9000/webhook/received`.
Чтобы получить уведомления на свой адрес, укажите в `webhook_url` URL с https://webhook.site.

### Проверка retry и DLQ

Укажите `"webhook_url": "http://webhook-mock:9000/webhook/fail"` (всегда отвечает 500).
В логах consumer (`docker compose logs -f consumer`) будут видны 3 попытки, после чего сообщение окажется в очереди `payments.new.dlq`. Это видно на вкладке *Queues* в панели RabbitMQ или командой:

```bash
docker compose exec rabbitmq rabbitmqctl list_queues name messages
```

### Проверка Outbox

```bash
docker compose stop rabbitmq
# создайте платёж: API ответит 202, событие останется в outbox
docker compose exec postgres psql -U payments -c "select id, published_at from outbox"
docker compose start rabbitmq
# через несколько секунд событие будет опубликовано и платёж обработан
```

## Ошибки API

| Код | Когда |
|---|---|
| 401 | нет или неверный `X-API-Key` |
| 404 | платёж не найден |
| 409 | `Idempotency-Key` уже использован с другим телом запроса |
| 422 | ошибка валидации или нет заголовка `Idempotency-Key` |

## Структура проекта

```
app/
  api/v1/          роутеры (payments) и подключение аутентификации
  core/            настройки, проверка X-API-Key, логирование
  db/              async engine и сессии
  models/          SQLAlchemy-модели payments и outbox
  schemas/         Pydantic-схемы API и сообщений
  repositories/    доступ к данным
  services/        бизнес-логика создания платежа (+ outbox, идемпотентность)
  broker/          подключение и топология RabbitMQ (exchange'и, очереди, DLX)
  outbox/relay.py  публикация событий из outbox
  consumer/        FastStream-приложение: шлюз, обработка, webhook с retry
  main.py          FastAPI-приложение
migrations/        Alembic
webhook_mock/      тестовый приёмник webhook'ов
tests/             pytest
```

## Настройки

Все параметры задаются переменными окружения (`app/core/config.py`):

| Переменная | По умолчанию |
|---|---|
| `API_KEY` | `super-secret-api-key` |
| `DATABASE_URL` | `postgresql+asyncpg://payments:payments@localhost:5432/payments` |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` |
| `OUTBOX_POLL_INTERVAL_SECONDS` | `1.0` |
| `GATEWAY_MIN_DELAY_SECONDS` / `GATEWAY_MAX_DELAY_SECONDS` | `2` / `5` |
| `GATEWAY_SUCCESS_RATE` | `0.9` |
| `WEBHOOK_MAX_ATTEMPTS` | `3` |
| `WEBHOOK_RETRY_BASE_DELAY_SECONDS` | `1.0` |
| `WEBHOOK_TIMEOUT_SECONDS` | `5.0` |

## Локальная разработка и тесты

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate    Linux/macOS: source .venv/bin/activate
pip install -r requirements-dev.txt

pytest          # тесты на SQLite, PostgreSQL и RabbitMQ не нужны
ruff check .
```

Запуск без Docker для приложения (PostgreSQL и RabbitMQ при этом нужны, например, `docker compose up postgres rabbitmq`):

```bash
cp .env.example .env
alembic upgrade head
uvicorn app.main:app --reload
faststream run app.consumer.main:app
```
