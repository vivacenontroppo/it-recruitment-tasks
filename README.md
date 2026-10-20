## Wymagania

- Docker + Docker Compose v2 (`docker compose`, nie `docker-compose`).
- Wolny port `9092` lokalnie (Kafka).
- Do pracy lokalnej nad testami (poza Dockerem): Node.js 20+.

## Uruchomienie środowiska

```bash
# Zbuduj obrazy i uruchom Kafkę + inicjalizację topiców + konsumenta
docker compose up --build -d

# Sprawdź logi konsumenta (powinien czekać na wiadomości)
docker compose logs -f consumer
```

## Ręczna demonstracja działania aplikacji (opcjonalnie, nie jest wymagane do zadania)

```bash
docker compose --profile demo run --rm producer
docker compose logs -f consumer
```

## Uruchomienie testów (tu pracujesz)

```bash
docker compose --profile test run --rm tests
```

### Uruchomienie testów lokalnie (bez Dockera dla samych testów)

Przydatne przy pisaniu/debugowaniu — szybszy cykl niż przebudowa obrazu Dockera:

```bash
cd tests
npm install
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 npm test
```

Sprawdzenie typów bez uruchamiania testów:

```bash
cd tests
npx tsc --noEmit
```

## Zatrzymanie i sprzątanie środowiska

```bash
docker compose down -v
```

(`-v` czyści też dane Kafki — przydatne, gdy chcesz zacząć testy od zera).


---

## Moje notatki z pracy nad projektem:

### Uruchomienie i konfiguracja
- Smoke (jedyny green path): `docker compose --profile test run --rm --build tests npm run test:smoke`
- DLQ (dwa red path'y): `docker compose --profile test run --rm --build tests npm run test:dlq`
- Całość z raportem w /tests/test-results: `docker compose --profile test run --rm --build tests npm run test:ci`

### Podejście do testowania
- Na początku problemem był race condition startu środowiska: test mógł ruszyć z obserwacją zanim Kafka i consumer były w pełni gotowe, a cleanup callbacków nie zawsze kończył się w przewidywalnym momencie i następował "hang". To poprawiłem w commitcie `b7f91f6`.
- Testy sprawdzają realny przepływ wiadomości pomiędzy `producer`, `consumer` i topicami `orders-processed` / `orders-dlq`.
- Dodatkowo uprościłem logikę helpera: `waitForMessage()` ma czekać na właściwą wiadomość, a nie powielać walidacji w teście (Commit `4cdfd12`)
- Zamiast śledzić tylko `orderId` w payloadzie, użyłem klucza Kafka jako wiarygodnego linku między żądaniem a odpowiedzią.

### Race condition i callbacki
Dodałem `consumer.stop()` i `consumer.disconnect()` po zakończeniu promisa z `setTimeout(..., 0)`, żeby cleanup następował w momencie w którym callback nie był już używany. Dzięki temu test przestał "wisieć".

### Problem ze starymi wiadomościami w DLQ
Po zmianie logiki `waitForMessage()`, która dublowała test, ponieważ szukała konkretnej wiadomości, natrafiłem na ciekawy problem:

```js
expect(received).toEqual(expected) // deep equality

- Expected  - 2
+ Received  + 3

- ObjectContaining {
-   "error": StringContaining "Brakujące pola",
+ Object {
+   "error": "Niepoprawny JSON: Unexpected end of JSON input",
+   "raw": "{\"broken\":",
  }
```

Helper czytał wiadomości od początku topicu i porównywał payload, więc stare wpisy z DLQ nadal spełniały warunek.
`order.orderId` z JSON nie tworzyło prawdziwego powiązania między żądaniem a odpowiedzią.

Dlatego finalnie dodałem do dopasowania metadane wiadomości:

```ts
metadata?: MessageMetadata
```

oraz użyłem `randomUUID()` do wygenerowania klucza. Dzięki temu test sprawdza właściwą wiadomość, a nie dowolny stary rekord z tego samego topicu.

### Co to dało
- stabilny flow happy path i DLQ,
- brak zbędnego duplikowania walidacji w testach - oczekiwanie na dowolną odpowiedź i sprawdzanie jej z wymaganiem, a nie szukanie po wymaganiu
- czytelny helper do oczekiwania na wiadomości,
- realne powiązanie request/response na poziomie Kafka key, nie na podstawie surowego payloadu
