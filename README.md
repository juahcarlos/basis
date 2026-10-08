# Асинхронный сервис процессинга платежей

FastAPI принимает платежи, PostgreSQL хранит их вместе с transactional outbox, RabbitMQ доставляет события consumer-процессу, а результат обработки отправляется на `webhook_url`.

## Поток обработки

1. Клиент отправляет запрос в API с `Idempotency-Key`.
2. API в одной транзакции записывает платёж в `payments` и событие `payment.new` в `outbox`.
3. Outbox publisher выбирает ожидающие события, публикует их в `payments.new` и помечает опубликованными только после publisher confirm от RabbitMQ.
4. Consumer получает событие, вызывает FakeGateway и сохраняет финальный статус платежа.
5. Consumer отправляет webhook с результатом обработки.
6. Ошибка обработки или доставки webhook отправляет сообщение на retry; после исчерпания попыток сообщение уходит в DLQ.

Outbox и webhook обеспечивают доставку **at-least-once**, а не exactly-once: сообщения и webhook могут быть доставлены повторно. Consumer сохраняет финальный статус до отправки webhook, поэтому повторная доставка уже завершённого платежа повторяет webhook, но не вызывает gateway снова. Два worker, одновременно прочитавшие `pending`, могут оба обратиться к gateway; DB-lock берётся только для короткой повторной проверки и записи результата. Между внешним gateway и PostgreSQL нет общей транзакции, поэтому реальный gateway обязан дедуплицировать вызовы по idempotency key; consumer передаёт ему ID платежа. FakeGateway этот ключ принимает, но не использует.

## Запуск

Требуются Docker Engine и Docker Compose. Приложение, Python-инструменты, тесты и миграции запускаются в контейнерах.

Скопируйте шаблон окружения:

```bash
cp .env.example .env
```

При первом запуске создайте `uv.lock` одноразовым контейнером с uv. Команда не собирает проект и не создаёт `.venv` на хосте:

```bash
docker run --rm -v "$PWD":/work -w /work ghcr.io/astral-sh/uv:python3.13-bookworm-slim uv lock
```

Поднимите весь стек:

```bash
docker compose up --build -d
```

Compose дождётся PostgreSQL и RabbitMQ, выполнит миграцию сервисом `migrate`, затем запустит API и consumer.

- Swagger: <http://localhost:8001/docs>
- Health: <http://localhost:8001/health>
- RabbitMQ Management: <http://localhost:15672>

Для локального окружения API опубликован на порту `8001` хоста и слушает `8000` внутри контейнера. Учетные данные сервисов задаются в `.env`; шаблон использует `payments` и `change-me`.

Остановить сервисы, не удаляя данные:

```bash
docker compose down
```

`docker compose down -v` дополнительно удаляет volume PostgreSQL и состояние RabbitMQ.

## API

Защищённые endpoints требуют `X-API-Key`, значение которого должно совпадать с `API_KEY` в `.env`. В приведённых примерах используется шаблонное значение `change-me`.

### Создать платёж

```bash
curl -i -X POST http://localhost:8001/api/v1/payments \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: change-me' \
  -H 'Idempotency-Key: example-payment-001' \
  -d '{"amount":"25.50","currency":"USD","description":"Invoice 001","metadata":{},"webhook_url":"https://example.test/webhook"}'
```

Ожидаемый ответ — `202 Accepted` с `payment_id`, `status` и `created_at`.

### Повторить создание с тем же ключом

Отправьте тот же запрос с теми же телом и `Idempotency-Key`: API вернёт тот же `payment_id`, не создавая новую outbox-запись. Если тело отличается, ответ будет `409 Conflict`.

### Получить платёж

Подставьте `payment_id` из ответа создания:

```bash
curl -i -H 'X-API-Key: change-me' \
  http://localhost:8001/api/v1/payments/<payment_id>
```

Ответ `200 OK` содержит поля платежа. Сумма сериализуется JSON-ом как строка, например `"25.50"`.

### Ошибка без ключа идемпотентности

```bash
curl -i -X POST http://localhost:8001/api/v1/payments \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: change-me' \
  -d '{"amount":"25.50","currency":"USD","description":"Invoice 001","metadata":{},"webhook_url":"https://example.test/webhook"}'
```

Без `Idempotency-Key` FastAPI вернёт `422 Unprocessable Content`. Без или с неверным API-ключом защищённые endpoints вернут `401 Unauthorized`; отсутствующий платёж — `404 Not Found`.

`/health` не требует API-ключа. Он проверяет запросом PostgreSQL и отдельным AMQP-подключением RabbitMQ; при недоступности любой зависимости отвечает `503`.

`description` ограничен 500 символами. `webhook_url` ограничен 2048 символами на уровне API-схемы, не обрезается. `metadata` ограничена 4096 байтами после компактной JSON-сериализации.

Webhook URL принимаются только для публичных host/IP адресов. Перед отправкой сервис повторно резолвит hostname и отклоняет адреса, которые не являются публичными; такой постоянный SSRF-отказ направляется сразу в DLQ, а временная ошибка DNS проходит обычные retry. Для строго доверенного локального адреса можно задать `WEBHOOK_ALLOWED_HOSTS` как JSON-массив точных пар `host:port`, например `WEBHOOK_ALLOWED_HOSTS=["127.0.0.1:9000"]`; по умолчанию список пуст. Wildcard и CIDR не поддерживаются. Allowlist обходит проверку публичности только для точного совпадения host и порта. DNS-проверка выполняется перед HTTP-запросом, но клиент затем резолвит имя самостоятельно: это не защищает от DNS rebinding между проверкой и соединением.

Каждый webhook подписывается секретом из `WEBHOOK_SIGNING_SECRET`: заголовки `X-Webhook-Timestamp` и `X-Webhook-Signature: sha256=<hex>`, где подпись это HMAC-SHA256 от строки `<timestamp>.<тело запроса>`. Получатель должен проверять подпись и отклонять запросы с устаревшим timestamp. Лимит размера тела запроса 64 KiB проверяется по заголовку `Content-Length`; запросы без него (chunked) этим лимитом не ограничиваются.

## Outbox, retry и DLQ

Outbox publisher опрашивает БД с интервалом `OUTBOX_POLL_INTERVAL_SECONDS`, выбирает до `OUTBOX_BATCH_SIZE` ожидающих событий и публикует их с устойчивой доставкой и `message_id`, равным ID outbox-записи. Подтверждение RabbitMQ фиксируется в отдельной короткой транзакции. В текущей реализации publisher должен работать в одном экземпляре: несколько экземпляров могут одновременно выбрать и опубликовать одну запись. Если RabbitMQ недоступен или публикация не подтверждена, запись останется `pending` для следующего цикла.

При значениях шаблона `RETRY_BASE_DELAY_SECONDS=2` и `MAX_ATTEMPTS=3` сообщение публикуется в `payments.retry.1` с `expiration=2_000`, в `payments.retry.2` с `expiration=4_000`, а после третьей неудачной обработки — в `payments.dlq` с заголовком `x-error-reason`. Статус платежа при ошибке webhook остаётся финальным.

Посмотреть сообщения можно в RabbitMQ Management: **Queues and Streams → payments.dlq → Get messages**. Для повторной обработки опубликуйте тело сообщения обратно в exchange `payments` с routing key `payments.new`; удаляйте сообщение из DLQ только после успешной публикации. Очереди `payments.new` и `payments.retry.*` создаются consumer при старте.

Для проверки успешного webhook используйте доступный HTTPS endpoint-приёмник, например URL, выданный webhook.site. Сервис из контейнера сам отправляет на него исходящий POST.

## Тесты и локальная разработка

Dev-окружение и все команды проекта работают в Docker. Интеграционные и E2E-тесты используют отдельную БД `payments_test` на PostgreSQL; при старте pytest эта БД пересоздаётся и к ней применяются Alembic-миграции. SQLite и Testcontainers не используются. Таблицы очищаются до и после каждого DB-backed теста. E2E-тесты входят в обычный прогон: pytest запускает API и consumer как subprocess, использует отдельный RabbitMQ vhost и локальный HTTP-приёмник webhook.

```bash
docker compose run --rm app pytest -q tests
docker compose run --rm app ruff check .
docker compose run --rm app mypy app
```

Для быстрого прогона без E2E-сценариев используйте `docker compose run --rm app pytest -q tests -m "not e2e"`.

Применить миграции вручную:

```bash
docker compose run --rm app alembic upgrade head
```

## Принятые решения

- Входной `description` ограничен 1–500 символами.
- Decimal отдаётся строкой в JSON, например `"25.50"`.
- API key, используемый в примерах `.env.example`, является тестовым значением, не production-секретом.
- FakeGateway ждёт 2–5 секунд и возвращает `succeeded` с вероятностью 90% либо `failed` с вероятностью 10%.
- Максимум три обработки включает исходную доставку; после третьей неудачи сообщение направляется в DLQ.
