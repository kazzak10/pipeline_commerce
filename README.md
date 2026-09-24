# Pipeline Commerce

Pipeline de données e-commerce combinant **streaming temps réel** et **traitement batch quotidien**.

Le projet simule des commandes e-commerce, les transmet via **Apache Kafka**, les traite avec **Apache Spark Structured Streaming**, les stocke dans **PostgreSQL**, puis utilise **Apache Airflow** pour produire un rapport journalier agrégé dans une couche analytique `gold`.

---

## Architecture

```text
                    ┌──────────────────────┐
                    │   Python Producer     │
                    │ Génération commandes  │
                    └──────────┬───────────┘
                               │
                               │ JSON
                               ▼
                    ┌──────────────────────┐
                    │      Apache Kafka    │
                    │ Topic: commandes     │
                    └──────────┬───────────┘
                               │
                               ▼
              ┌────────────────────────────────┐
              │ Apache Spark Structured        │
              │ Streaming                      │
              │                                │
              │ - Lecture Kafka                │
              │ - Désérialisation JSON         │
              │ - Structuration du schéma      │
              │ - Conversion du timestamp      │
              │ - Écriture JDBC                │
              └───────────────┬────────────────┘
                              │
                              ▼
                    ┌──────────────────────┐
                    │      PostgreSQL      │
                    │                      │
                    │ silver.commandes_live│
                    └──────────┬───────────┘
                               │
                               │ Traitement quotidien
                               ▼
                    ┌──────────────────────┐
                    │       Airflow        │
                    │ daily_batch_report   │
                    └──────────┬───────────┘
                               │
                               │ Pandas / groupby
                               ▼
                    ┌──────────────────────┐
                    │      PostgreSQL      │
                    │ gold.rapport_        │
                    │ journalier           │
                    └──────────────────────┘
```

---

## Objectif

L'objectif du projet est de mettre en œuvre un pipeline de données capable de :

1. Générer continuellement des commandes e-commerce simulées.
2. Publier ces commandes dans un flux Kafka.
3. Consommer et transformer le flux avec Spark Structured Streaming.
4. Stocker les événements traités dans PostgreSQL.
5. Extraire quotidiennement les commandes de la veille.
6. Agréger les données par produit avec Pandas.
7. Charger le résultat dans une table analytique dédiée.

Le pipeline combine donc deux approches :

- **Streaming** pour l'ingestion et le traitement continu des commandes.
- **Batch** pour la production du rapport journalier.

---

## Stack technique

| Technologie                       | Rôle                                                           |
| --------------------------------- | -------------------------------------------------------------- |
| Python                            | Génération des données et logique applicative                  |
| Apache Kafka                      | Messagerie et transport des événements                         |
| Apache Spark Structured Streaming | Traitement des données en streaming                            |
| PostgreSQL                        | Stockage des données                                           |
| Pandas                            | Nettoyage et agrégation du traitement batch                    |
| Apache Airflow                    | Orchestration du traitement quotidien                          |
| Docker Compose                    | Conteneurisation et exécution de l'ensemble de l'environnement |

---

## Structure du projet

```text
pipeline_commerce/
│
├── airflow/
│   └── dags/
│       └── daily_batch_report.py
│
├── producer/
│   ├── dockerfile
│   ├── producer.py
│   └── requirements.txt
│
├── spark_jobs/
│   └── streaming_job.py
│
├── sql/
│   └── init.sql
│
├── .env.example
├── .gitignore
├── docker-compose.yaml
└── README.md
```

---

## 1. Génération des commandes

Le fichier `producer/producer.py` génère automatiquement des commandes e-commerce.

Chaque commande contient :

```json
{
  "commande_id": "CMD-...",
  "produit": "Casque audio",
  "prix_unitaire": 79.99,
  "quantite": 2,
  "montant_total": 159.98,
  "ville": "PARIS",
  "timestamp": "2026-09-24T15:00:00+00:00"
}
```

Les produits et les villes sont sélectionnés aléatoirement.

Le montant total est calculé à partir du prix unitaire et de la quantité :

```python
montant_total = prix * quantite
```

Le producteur envoie ensuite chaque commande dans le topic Kafka :

```text
commandes
```

Une nouvelle commande est générée toutes les 4 secondes.

---

## 2. Apache Kafka

Kafka constitue la couche de transport des événements entre le producteur et Spark.

Le producer Python utilise :

```text
kafka:9092
```

et publie les commandes dans :

```text
Topic : commandes
```

Dans l'environnement Docker actuel, le projet utilise :

- 1 broker Kafka
- 1 instance ZooKeeper
- un topic `commandes`

Kafka joue ici le rôle de **message broker** permettant de découpler la génération des données de leur traitement.

---

## 3. Spark Structured Streaming

Le fichier :

```text
spark_jobs/streaming_job.py
```

met en place un traitement avec **Spark Structured Streaming**.

Spark lit les messages du topic Kafka :

```python
.readStream
.format("kafka")
.option("subscribe", "commandes")
.load()
```

Les valeurs Kafka sont ensuite converties depuis leur représentation binaire vers du texte JSON.

Un schéma Spark est défini pour typer les données :

```python
SCHEMA = StructType([
    StructField("commande_id", StringType()),
    StructField("produit", StringType()),
    StructField("prix_unitaire", FloatType()),
    StructField("quantite", IntegerType()),
    StructField("montant_total", FloatType()),
    StructField("ville", StringType()),
    StructField("timestamp", StringType()),
])
```

Le JSON est désérialisé avec :

```python
from_json()
```

puis le timestamp est converti en type temporel Spark :

```python
to_timestamp()
```

Le DataFrame obtenu représente alors les commandes structurées prêtes à être persistées.

---

## 4. Écriture dans PostgreSQL

Les données issues de Spark sont écrites dans PostgreSQL grâce au connecteur JDBC.

La table cible est :

```text
silver.commandes_live
```

Le projet utilise deux schémas PostgreSQL :

```text
silver
gold
```

### Couche Silver

La table :

```text
silver.commandes_live
```

contient les commandes issues du flux de streaming.

Structure :

| Colonne         | Type          |
| --------------- | ------------- |
| `commande_id`   | VARCHAR(64)   |
| `produit`       | VARCHAR(128)  |
| `prix_unitaire` | NUMERIC(10,2) |
| `quantite`      | INTEGER       |
| `montant_total` | NUMERIC(10,2) |
| `ville`         | VARCHAR(64)   |
| `timestamp`     | TIMESTAMPTZ   |

`commande_id` constitue la clé primaire.

---

## 5. Traitement batch avec Airflow

Le DAG :

```text
airflow/dags/daily_batch_report.py
```

est chargé par Apache Airflow.

Le DAG est planifié quotidiennement à :

```text
03:00
```

avec :

```python
schedule="0 3 * * *"
```

Le traitement concerne les commandes de la veille.

La requête SQL récupère les données depuis :

```text
silver.commandes_live
```

pour la date précédente.

---

## 6. Nettoyage et agrégation avec Pandas

Les données extraites de PostgreSQL sont chargées dans un DataFrame Pandas.

L'agrégation est réalisée avec `groupby()` :

```python
rapport = df.groupby("produit").agg(
    nombre_commandes=("commande_id", "count"),
    chiffre_affaires=("montant_total", "sum")
).reset_index()
```

Le pipeline calcule donc, pour chaque produit :

- le nombre de commandes ;
- le chiffre d'affaires généré.

Une colonne supplémentaire est ensuite ajoutée :

```text
date_rapport
```

qui correspond à la date du rapport.

Exemple de résultat :

| produit           | nombre_commandes | chiffre_affaires | date_rapport |
| ----------------- | ---------------: | ---------------: | ------------ |
| Casque audio      |               15 |          1199.85 | 2026-09-23   |
| Clavier mécanique |               12 |          1558.80 | 2026-09-23   |
| Écran 27 pouces   |                9 |          2241.00 | 2026-09-23   |
| Souris sans fil   |               18 |           621.00 | 2026-09-23   |

---

## 7. Couche Gold

Le rapport agrégé est chargé dans :

```text
gold.rapport_journalier
```

Structure :

| Colonne            | Type          |
| ------------------ | ------------- |
| `id`               | SERIAL        |
| `produit`          | VARCHAR(128)  |
| `nombre_commandes` | INTEGER       |
| `chiffre_affaires` | NUMERIC(12,2) |
| `date_rapport`     | DATE          |

Une contrainte d'unicité est définie sur :

```text
(produit, date_rapport)
```

Cette table constitue la couche analytique finale du pipeline.

---

## 8. Orchestration

Airflow orchestre le traitement batch selon trois étapes logiques :

```text
extract()
    ↓
clean_and_agregate()
    ↓
load_report()
```

### Extract

Extraction des commandes de la veille depuis PostgreSQL.

### Clean & Aggregate

Transformation des données avec Pandas et agrégation avec `groupby()`.

### Load

Chargement du DataFrame agrégé dans :

```text
gold.rapport_journalier
```

---

## 9. Docker

L'ensemble de l'infrastructure est défini dans :

```text
docker-compose.yaml
```

Les services utilisés sont :

```text
postgres
zookeeper
kafka
producer
spark
spark-worker
airflow-init
airflow-webserver
airflow-scheduler
```

Docker Compose permet de lancer l'environnement de manière reproductible sans installer individuellement chaque composant.

---

## 10. Configuration

Créer le fichier `.env` à partir de `.env.example` :

```bash
cp .env.example .env
```

Configuration actuelle :

```env
POSTGRES_USER=ecommerce
POSTGRES_PASSWORD=changeme
POSTGRES_DB=ecommerce
```

Il est recommandé de modifier le mot de passe avant une utilisation hors environnement de développement.

---

## 11. Installation et lancement

Cloner le projet :

```bash
git clone https://github.com/kazzak10/pipeline_commerce.git
cd pipeline_commerce
```

Créer la configuration :

```bash
cp .env.example .env
```

Lancer l'ensemble des services :

```bash
docker compose up --build
```

Lancer en arrière-plan :

```bash
docker compose up --build -d
```

Vérifier l'état des conteneurs :

```bash
docker compose ps
```

Consulter les logs :

```bash
docker compose logs -f
```

---

## 12. Interfaces

### Spark Master

```text
http://localhost:8081
```

### Airflow

```text
http://localhost:8082
```

Les identifiants créés par la configuration actuelle sont :

```text
username: airflow
password: airflow
```

Ces identifiants sont destinés à l'environnement de développement et doivent être modifiés pour un environnement sécurisé.

---

## 13. Flux de données complet

Une commande suit le chemin suivant :

```text
1. Génération de la commande
          ↓
2. Sérialisation JSON
          ↓
3. Publication dans Kafka
          ↓
4. Consommation par Spark Structured Streaming
          ↓
5. Désérialisation et typage du JSON
          ↓
6. Conversion du timestamp
          ↓
7. Écriture JDBC dans PostgreSQL
          ↓
8. Stockage dans silver.commandes_live
          ↓
9. Extraction quotidienne par Airflow
          ↓
10. Chargement dans un DataFrame Pandas
          ↓
11. Agrégation par produit
          ↓
12. Calcul du nombre de commandes
          ↓
13. Calcul du chiffre d'affaires
          ↓
14. Écriture dans gold.rapport_journalier
```

---

## 14. Concepts Data Engineering mis en œuvre

Ce projet met en pratique plusieurs concepts fondamentaux du Data Engineering :

- **Event Streaming**
- **Message Broker**
- **Streaming Ingestion**
- **Spark Structured Streaming**
- **Data Serialization / Deserialization**
- **Schema Enforcement**
- **JDBC Data Integration**
- **Relational Data Storage**
- **Data Transformation**
- **Batch Processing**
- **Data Aggregation**
- **Workflow Orchestration**
- **Data Layering (`silver` / `gold`)**
- **Containerisation avec Docker**

---

## 15. Technologies et responsabilités

```text
Python
→ génération des événements

Kafka
→ transport des événements

Spark Structured Streaming
→ ingestion et transformation du flux

PostgreSQL
→ stockage des données opérationnelles et analytiques

Pandas
→ transformation et agrégation batch

Airflow
→ orchestration du traitement quotidien

Docker Compose
→ déploiement local de l'ensemble de l'écosystème
```

---

## 16. Limites actuelles

Le projet est actuellement conçu comme un environnement de démonstration et d'apprentissage.

L'infrastructure Kafka repose notamment sur un seul broker et le stockage PostgreSQL est local via Docker.

Le pipeline peut ensuite être étendu avec :

- plusieurs brokers Kafka ;
- plusieurs partitions et une stratégie de réplication ;
- gestion des erreurs et des données invalides ;
- tests automatisés ;
- monitoring et alerting ;
- gestion plus robuste des secrets ;
- CI/CD ;
- visualisation des données avec un outil BI ;
- optimisation des traitements Spark ;
- stratégie d'idempotence et de reprise sur incident.

---

## Auteur

**Zakaria Houari**

Projet réalisé dans le cadre d'une mise en pratique des architectures modernes de **Data Engineering** et du traitement de données en **streaming et batch**.
