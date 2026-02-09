# Projekt-Data-Engineering
Batch-basierte Datenarchitektur für eine datenintensive Applikation

## Voraussetzungen

```
docker network create data-engineering
``` 

## Kafka (Kraft)

Dieser Container startet Apache Kafka im KRaft-Modus (ohne ZooKeeper) als Single-Node-Setup, bei dem Broker und Controller im selben Prozess laufen.
Er ist für lokale Entwicklung und Tests optimiert und erlaubt Zugriff sowohl vom Host als auch von anderen Containern im selben Docker-Netzwerk. Daten werden persistent über ein Docker Volume gespeichert. Für einen produktiven Einsatz sollten seperate Broker und Controller verwendet werden und Sicherheitsmechanismen verwendet werden (z.B. TLS). Das Image von Confluent wird verwendet, da ich schon viel Erfahrung mit diesem habe. 

### Netzwerk & Zugriff

Vom Host: localhost:29092
Von anderen Containern: kafka:9092
Controller-Kommunikation: intern über 9093

### AKHQ

Hier wird AKHQ als leichtgewichtige Web-Oberfläche zur Inspektion und Verwaltung von Kafka verwendet. AKHQ ermöglicht es, Topics, Partitionen, Offsets und Nachrichten per Browser einzusehen.

```
http://localhost:8080/
``` 

## Beispieldaten

https://www.kaggle.com/datasets/prasad22/weather-data

