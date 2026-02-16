# Projekt-Data-Engineering

## Kurzes Abstract

Dieses Projekt entwickelt eine batch-basierte Datenpipeline zur Verarbeitung von Wetterdaten aus einem Kaggle-Datensatz mit mehreren Standorten. Die Architektur nutzt Kafka zur Entkopplung, speichert Rohdaten append-only und berechnet Tagesdurchschnitte pro Ort. Aggregierte Ergebnisse werden in PostgreSQL gespeichert und über ein Streamlit-Dashboard visualisiert.

![Pipeline](/diagram/diagram_p2.png)

## Komponenten

### Kafka (Kraft)

Dieser Container startet Apache Kafka im KRaft-Modus (ohne ZooKeeper) als Single-Node-Setup, bei dem Broker und Controller im selben Prozess laufen.
Er ist für lokale Entwicklung und Tests optimiert und erlaubt Zugriff sowohl vom Host als auch von anderen Containern im selben Docker-Netzwerk. Daten werden persistent über ein Docker Volume gespeichert. Für einen produktiven Einsatz sollten seperate Broker und Controller verwendet werden und Sicherheitsmechanismen verwendet werden (z.B. TLS). Das Image von Confluent wird verwendet, da ich schon viel Erfahrung mit diesem habe. 

### AKHQ

Hier wird AKHQ als leichtgewichtige Web-Oberfläche zur Inspektion und Verwaltung von Kafka verwendet. AKHQ ermöglicht es, Topics, Partitionen, Offsets und Nachrichten per Browser einzusehen. Zugriff über:

```
http://localhost:8080/
``` 

### Kafka Producer

Der Kafka Producer liest Daten aus der Datenquelle (`/data/weather_data.csv`) und schreibt sie in ein Kafka Topic (`weather_data_raw`). Davor wird überprüft, ob das Topic bereits vorhanden ist. Wenn das Topic noch nicht existiert, wird es erstellt. 

### Kafka Consumer

Der Kafka Consumer liest Daten aus dem Kafka Topic (`weather_data_raw`) und speichert diese in ein append-only Verzeichnis `/raw` (Raw Storage Zone) als persistenten Speicher. Die Daten werden dabei als NDJSON gespeichert. Jede Nachricht entspricht eine Zeile der NDJSON. Neben dem Event-Value werden auch Kafka-Metadaten gespeichert (Topic, Partition, Offset, Timestamp, Key). Um sehr große Dateien zu vermeiden, implementiert der Consumer eine Dateirotation. Die Datei wird rotiert, wenn sich das Datum ändert oder eine bestimmte Dateigröße überschritten (ROTATE_MB) wird.

### Batch Processing Job

Der Batch Processing Job holt sich read-only alle NDJSON Dateien aus `/raw` und verarbeitet diese. Die Daten in den Dateien werden Zeile für Zeile gelesen und verarbeitet. Es wird für jeden Ort und Tag der Durchschnittliche Wert für Temperatur, Luftfeuchtigkeit und Windgeschwindigkeit berechnet, sowie die Niederschlagsmenge. Diese aggregierten Daten werden dann nach Postgres geschrieben.

### Postgres

In der Postgres Datenbank werden die aggregierten Daten gespeichert (Processed Zone).

### Streamlit Dashboard

Das Streamlit Dashboard visualisiert die aggregierten Daten aus der Datenbank und berechnet die Durchschnittstemperatur, Luftfeuchtigkeit und Windgeschwindigkeit. Zugriff über:

```
http://localhost:8501/
``` 

## Verwendete Daten

Für dieses Projetzt wurden synthetische Wetterdaten für die Städte New York, Los Angeles, Chicago, Houston, Phoenix, Philadelphia, San Antonio, San Diego, Dallas, and San Jose verwendet. Diese Daten stammen von [Kaggle](https://www.kaggle.com/datasets/prasad22/weather-data) und bestehen aus einer Millionen Datenpunkten.


## How to run

### Befehle

Damit die Pipeline läuft müssen folgende zwei Befehle ausgeführt werden. Der erste um alle Services zu starten außer den Batch Job.
```
docker compose up (-d für detached)
``` 
Und dieser um den Batch Jopb zu starten. 
``` 
docker compose --profile batch up batch-processing (-d für detached)
``` 

Zum stoppen:
```
docker compose -p projekt-data-engineering down (-v um alle Volumes zu löschen)
```

Um auf den Startzustand zurück zu kommen, müssen alle Volumes gelöscht werden und die Daten in der Raw-Zone entfernt werden.

### Ablauf

Es gibt natürliche einen Unterschied, ob man die Services im Labor laufen lässt oder bei normalen Betrieb. Hier die größten Unterschiede:

Im Lab:

1. Kafka Producer produziert **alle** Wetterdaten und sendet sie nach Kafka. Danach beendet sich der Service.
2. Kafka Consumer liest jede Nachricht und schreib sie als neue Zeile in die Raw Storage Zone. Neue Nachrichten werden ebenfalls angefügt.
3. Der Batch Job wird per Hand gestartet und aggregiert die Daten. Wenn sich aggregierte Daten ändern, werden sie in der Datenbank überschrieben.
4. Die Daten können verwendet werden.

Im normalen Betrieb:

1. Kafka Producer produziert jeden Tag Wetterdaten und sendet sie nach Kafka.
2. Kafka Consumer liest jede Nachricht und schreib sie als neue Zeile in die Raw Storage Zone. Neue Nachrichten werden ebenfalls angefügt.
3. Der Batch Job läuft regelmäßig oder wird per Hand gestartet und aggregiert die Daten. Wenn sich aggregierte Daten ändern, werden sie in der Datenbank überschrieben.
4. Die Daten können verwendet werden.

## Qualitätsanforderungen

### Verlässlichkeit

Die Verarbeitung erfolgt at-least-once, da Kafka-Offets erst nach erfolgreichem Persistieren der Daten committed werden. Durch append-only Speicherung und idempotente Batch-Aggregation wird Datenverlust sowie inkonsistente Verarbeitung vermieden.

### Skalierbarkeit

Kafka ermöglicht horizontale Skalierung über Partitionierung und parallele Consumer-Instanzen. Die entkoppelte Architektur (Streaming, Batch, Datenbank, Dashboard) erlaubt unabhängige Skalierung einzelner Komponenten.

### Wartbarkeit

Die klare Trennung zwischen Producer, Consumer, Batch-Verarbeitung, Kafka und Postgres erhöht die Modularität und Erweiterbarkeit. Alle Services sind containerisiert und über Umgebungsvariablen konfigurierbar.

### Datensicherheit

Die Services laufen isoliert in einem internen Docker-Netzwerk, wodurch unautorisierter Zugriff minimiert wird. Die Raw Zone ist append-only strukturiert und schützt vor unbeabsichtigten Datenmanipulationen.

### Data Governance

Die Trennung in Raw- und Processed-Zone stellt Nachvollziehbarkeit und Reproduzierbarkeit sicher. Kafka-Metadaten werden mitgespeichert, sodass Datenherkunft und Verarbeitungsschritte transparent bleiben.

### Datenschutz

Es werden ausschließlich nicht-personenbezogene Wetterdaten verarbeitet. Bei realen Daten könnten Zugriffskontrollen, Rollenmodelle und Verschlüsselung ergänzt werden.
