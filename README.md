# Uruchomienie

```
docker compose up -d
```

Uwaga: po uruchomieniu główna aplikacja (scala-app) czeka 30s na pełne uruchomienie się Cassandry.

# Architektura i założenia

Rozwiązanie ma filtrować SMS-y dla wszystkich chętnych użytkowników sieci.
Zakładam, że sieć jest duża (miliony użytowników) i że chętnych użytkowników
może również być dużo. W związku z tym rozwiązanie zakłada skalowanie do wielu
węzłów, chociaż testowa konfiguracja działa na jednym węźle.

## Kafka

Dane wejściowe i wyjściowe (SMS-y) są pobierane z i wysyłane do topiców Kafka.
SMS-y pobierane są z topica `sms-input`. SMS-y, które powinny zostać dostarczone
użytkownikowi trafiają do topica `sms-output`, a te które zostały odrzucone
przez filtr, do topica `sms-rejected`.

### Dlaczego Kafka?

Ze względu na dobrą skalowalność do wielu węzłów i możliwość przetwarzania danych w "czasie rzeczywistym".

## Cassandra

Informacja o tym czy dany użytkownik korzysta z usługi, czy z niej zrezygnował
trzymana jest w bazie danych Cassandra.

W bazie Cassandra trzymany jest także cache sprawdzonych adresów dla
uniknięcia niepotrzebnych opłat.

### Dlaczego Cassandra?

Poza wspomnianą już skalowalnością do wielu węzłów, ważne jest, żeby informacja o włączeniu/wyłączeniu usługi nie
zginęła, nawet w przypadku awarii jednego z węzłów (przynajmniej po tym, jak użytkownik otrzyma potwierdzenie).
Dlatego podczas zapisu
używany jest tryb spójności "quorum". Ważne jest też, żeby informacja ta była dostępna dla
wszystkich instancji aplikacji, ale aktualizacje nie muszą być natychmiast
widoczne dla każdej instancji ("eventual consistency" jest akceptowalne).

Cache trzymany jest w Cassandrze "przy okazji". Dla cache-a utrata zapisu w przypadku
awarii nie jest dużym problemem. Wspólny cache dla wszystkich instancji
aplikacji jest plusem, ale też nie jest niezbędny.  Dlatego w przypadku cache-a
możnaby zastosować też inne rozwiązanie, np. Redis (z replikacją lub bez).

