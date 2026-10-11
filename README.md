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

### Соединения между контейнерами

Все сервисы находятся в одной сети Docker Compose и обращаются друг к другу по именам сервисов (`postgres`, `rabbitmq`, `webhook-mock`). Порты из секции `ports:` нужны только для доступа с хоста.

```
 хост (ваш компьютер)
   │ ① HTTP  localhost:8000 ──(проброс порта)──┐
   │                                            ▼
   │                                   ┌──────────────┐ ② TCP 5432 (протокол PostgreSQL)
   │                                   │     api      │───────────────────────┐
   │                                   │ uvicorn +    │ ③ TCP 5672 (AMQP)     │
   │                                   │ outbox relay │──────────┐            │
   │                                   └──────────────┘          ▼            ▼
   │                                                      ┌──────────┐  ┌──────────┐
   │                                                      │ rabbitmq │  │ postgres │
   │                                                      └──────────┘  └──────────┘
   │                                                           ▲ ④ AMQP     ▲ ⑤ TCP 5432
   │                                                           │            │
   │                                                      ┌──────────────────┐
   │                                                      │    consumer      │
   │                                                      └──────────────────┘
   │                                                           │ ⑥ HTTP webhook-mock:9000
   │                                                           ▼
   │                                                      ┌──────────────┐
   └── localhost:9000 (только чтобы смотреть /received) ─►│ webhook-mock │
                                                          └──────────────┘
```

| № | Откуда → куда | Протокол / порт | Для чего |
|---|---|---|---|
| ① | хост → `api` | HTTP, `localhost:8000` | запросы клиента к API |
| ② | `api` → `postgres` | PostgreSQL, 5432 | запись платежей и outbox, чтение outbox relay'ем |
| ③ | `api` → `rabbitmq` | AMQP, 5672 | публикация событий из outbox (с publisher confirms) |
| ④ | `consumer` ↔ `rabbitmq` | AMQP, 5672 | получение сообщений из `payments.new`, ack / nack |
| ⑤ | `consumer` → `postgres` | PostgreSQL, 5432 | чтение платежа и обновление статуса |
| ⑥ | `consumer` → `webhook-mock` | HTTP, 9000 | отправка webhook (или во внешний интернет, если `webhook_url` внешний) |

- TCP-соединения открывают только клиенты (`api`, `consumer`). RabbitMQ не подключается к consumer сам, а присылает сообщения по соединению ④, которое consumer открыл при старте. Соединения ②–⑤ долгоживущие (пулы БД и AMQP) и не создаются заново на каждый платёж.
- `api` и `consumer` не общаются напрямую: их связывают только PostgreSQL и RabbitMQ.
- Webhook отправляет контейнер `consumer`, поэтому `localhost` в `webhook_url` указывает на сам consumer, а не на ваш компьютер. Для тестового приёмника используйте `http://webhook-mock:9000/webhook`.

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