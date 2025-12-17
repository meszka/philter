# Uruchomienie

```
docker compose up -d
```

Uwaga: po uruchomieniu główna aplikacja (scala-app) czeka 30s na pełne uruchomienie się Cassandry.

# Konfiguracja

Aplikacja przyjmuje ustawienia w formie zmiennych środowiskowych. Można zmienić ich wartości w pliku `docker-compose.yml`:

- `GOOGLE_API_KEY` - klucz API dla usługi `https://cloud.google.com/web-risk/docs/reference/rest/v1eap1/TopLevel/evaluateUri`. Domyślna wartość `fake` powoduje, że używana jest aatrapa - aplikacja podczas weryfikacji adresu czeka 1s i uznaje adres za niebezpieczny jeśli zawiera ciąg znaków `m-bonk`
- `URL_CACHE_TTL_SECONDS` - TTL dla cache-a sprawdzonych adresów w sekundach (domyślnie 24h)
- `OPT_IN_NUMBER` - numer na który klient wysyła wiadomość `START` lub `STOP`

# Testowanie

## Testy jednostkowe

```
sbt test
```


## Testowanie ręczne

Można przetestować rozwiązanie korzystając ze skryptów Kafka (po uprzednim pobraniu i rozpakowaniu):

### Włączenie usługi
```
cat start.jsonl | ~/kafka_2.13-4.1.1/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic sms-input
```

### Przykładowe SMS-y do filtrowania

```
cat example-sms.jsonl | ~/kafka_2.13-4.1.1/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic sms-input
```

### Wyłączenie usługi
```
cat stop.jsonl | ~/kafka_2.13-4.1.1/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic sms-input
```

### Podejrzenie SMS-ów do dostarczenia
```
~/kafka_2.13-4.1.1/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sms-output --from-beginning
```

### Podejrzenie odrzuconych SMS-ów
```
~/kafka_2.13-4.1.1/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sms-rejected --from-beginning
```

# Architektura i założenia

Rozwiązanie ma filtrować SMS-y dla wszystkich chętnych użytkowników sieci.
Zakładam, że sieć jest duża (miliony użytowników) i że chętnych użytkowników
może również być dużo. W związku z tym rozwiązanie zakłada skalowanie do wielu
węzłów, chociaż testowa konfiguracja (zdefiniowana w `docker-compose.yml`)
działa na jednym węźle.

## Kafka

Dane wejściowe i wyjściowe (SMS-y) są pobierane z i wysyłane do topiców Kafka.
SMS-y pobierane są z topica `sms-input`. SMS-y, które powinny zostać dostarczone
użytkownikowi trafiają do topica `sms-output`, a te które zostały odrzucone
przez filtr, do topica `sms-rejected`.

### Dlaczego Kafka?

Ze względu na dobrą skalowalność do wielu węzłów i możliwość przetwarzania danych w "czasie rzeczywistym".

## Cassandra

Informacja o "opt-in", czyli o tym czy dany użytkownik korzysta z usługi, czy z niej zrezygnował
trzymana jest w bazie danych Cassandra.

W bazie Cassandra trzymany jest także cache sprawdzonych adresów dla
uniknięcia niepotrzebnych opłat.

### Dlaczego Cassandra?

Poza wspomnianą już skalowalnością do wielu węzłów, ważne jest, żeby informacja
o włączeniu/wyłączeniu usługi nie ginęła, nawet w przypadku awarii jednego z
węzłów (przynajmniej po tym, jak użytkownik otrzyma potwierdzenie). Dlatego
podczas zapisu używany jest tryb spójności "quorum". Ważne jest też, żeby
informacja ta była dostępna dla wszystkich instancji aplikacji, ale
aktualizacje nie muszą być natychmiast widoczne dla każdej instancji ("eventual
consistency" jest akceptowalne).

Cache trzymany jest w Cassandrze "przy okazji" - żeby uniknąć komplikowania architektury dodając kolejny element. Dla
cache-a utrata zapisu w przypadku
awarii nie jest dużym problemem. Wspólny cache dla wszystkich instancji
aplikacji jest plusem, ale też nie jest niezbędny. Dlatego w przypadku cache-a
możnaby zastosować też inne rozwiązanie, np. Redis (z replikacją lub bez).


## Kolejność wiadomości

Założyłem, że aplikacja nie musi zachowywać kolejności wiadomości, natomiast kolejność wiadomości dla danej pary
(nadawca, odbiorca) może mieć znaczenie dla klientów. Można ten problem rozwiązać w
jednym z dwóch miejsc:

a) w **tej aplikacji** wprowadzając klucze partycji Kafki odpowiadające parom (nadawca, odbiorca) i dbając o to, by
wiadomości dla danego klucza (lub całej partycji) były przetwarzane w kolejności

b) w **części systemu odpowiedzialnym za dostarczanie wiadomości** dodając czas nadania wiadomości do danych o SMSie


# Możliwe zmiany/usprawnienia

## Ponawianie zapytań

Przydałby się mechanizm ponawiania zapytania do usługi weryfikującej bezpieczeństwo linków.
W tej chwili w przypadku niepowodzenia, link jest uznawany za bezpieczny (ale nie trafia do cache-a).

## Wykorzystanie Kafka Streams i KTable

Można rozważyć zmianę z korzystania z podstawowego API Kafki na korzystanie z Kafka Streams. Wyższy poziom abstrakcji
Kafka Streams mógłby uprościć kod, zwłaszcza gdyby przyjąć założenie, że konieczne jest zachowanie kolejności wiadomości.
KTable możnaby użyć do przechowywania stanu "opt-in" zamiast Cassandry.
