# 🏅 Medallion Pipeline

> Pipeline de données ETL structuré en architecture Medallion (Bronze / Silver / Gold)  
> Implémenté avec PySpark sur un dataset public d'accidents de la route en France.

---

## 🏗️ Architecture Medallion

L'architecture Medallion organise les données en **3 couches progressives**, chacune augmentant la qualité et la valeur de la donnée.

```
📥 Source brute (CSV)
       │
       ▼
┌─────────────────────────────────────────────────┐
│  🟤 BRONZE — Raw Layer                          │
│  Données ingérées telles quelles                │
│  Aucune transformation — traçabilité maximale   │
└─────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────┐
│  ⚪ SILVER — Cleaned Layer                      │
│  Nettoyage, typage, renommage des colonnes      │
│  Suppression des nulls critiques                │
│  Correction des formats (ex: décimaux français) │
└─────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────┐
│  🟡 GOLD — Business Layer                       │
│  Agrégations métier prêtes à consommer          │
│  Datasets optimisés pour dashboards / rapports  │
└─────────────────────────────────────────────────┘
```

## 📖 Pour aller plus loin

> 💡 Architecture Medallion - concept introduit par Databricks :  
> [Building Data Pipelines with Delta Lake - Databricks](https://www.databricks.com/glossary/medallion-architecture)

---

### Pourquoi cette architecture ?

| Problème | Solution Medallion |
|---|---|
| La donnée brute est fragile | Bronze garde une copie intacte, immuable |
| Un bug en transformation détruit tout | On peut rejouer Silver depuis Bronze |
| Difficile de déboguer un ETL classique | Chaque couche est inspectable indépendamment |
| Pas de traçabilité | Historique complet à chaque étape |

---

## 📊 Dataset

**Source** : [data.gouv.fr - Accidents corporels de la circulation routière 2023](https://www.data.gouv.fr/fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2023/)

**Fichier utilisé** : `caracteristiques_2023.csv` - 54 822 accidents recensés en France

**Colonnes principales** : identifiant accident, date, département, conditions météo, luminosité, type de collision, coordonnées GPS

---

## 🛠️ Stack technique

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Poetry](https://img.shields.io/badge/Poetry-60A5FA?style=flat&logo=poetry&logoColor=white)
![pytest](https://img.shields.io/badge/pytest-0A9EDC?style=flat&logo=pytest&logoColor=white)

---

## 📁 Structure du projet

```
medallion-pipeline/
├── medallion/
│   ├── bronze/
│   │   └── ingest.py        # Lecture CSV → Parquet brut
│   ├── silver/
│   │   └── transform.py     # Nettoyage, typage, renommage
│   └── gold/
│       └── aggregate.py     # Agrégations métier
├── tests/
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
├── data/
│   └── raw/                 # Dataset source (non versionné)
├── output/                  # Fichiers Parquet générés (non versionné)
│   ├── bronze/
│   ├── silver/
│   └── gold/
│       ├── by_month/
│       ├── by_department/
│       └── by_weather/
├── run_pipeline.py          # Point d'entrée du pipeline
└── pyproject.toml
```

---

## ⚙️ Lancer le projet

### Prérequis
- Python 3.10+
- Poetry
- Java 11+ (requis par PySpark)

### Installation

```bash
git clone https://github.com/Maeva-RODRIGUES/medallion-pipeline
cd medallion-pipeline
poetry install
```

### Télécharger le dataset

Télécharge `caracteristiques_2023.csv` depuis [data.gouv.fr](https://www.data.gouv.fr/fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2023/) et place-le dans `data/raw/`.

### Lancer le pipeline

```bash
poetry run python run_pipeline.py
```

### Résultat attendu

```
🟤 Bronze : ingestion...
✅ Bronze : 54822 lignes ingérées

⚪ Silver : transformation...
✅ Silver : 54822 lignes après nettoyage

🟡 Gold : agrégation...
✅ Gold — accidents par mois     : 12 lignes
✅ Gold — accidents par département : 107 lignes
✅ Gold — accidents par météo    : 10 lignes

✅ Pipeline complet Bronze → Silver → Gold terminé !
```

---

## 🔍 Ce que fait chaque couche

### 🟤 Bronze - Ingestion brute
- Lecture du CSV source avec PySpark
- Stockage en Parquet sans aucune modification
- Principe : **ne jamais altérer la donnée d'origine**

### ⚪ Silver - Nettoyage
- Suppression des lignes avec valeurs critiques manquantes
- Renommage des colonnes (ex: `Num_Acc` → `accident_id`)
- Typage explicite (string → int, double)
- Correction des formats locaux (séparateur décimal `,` → `.`)
- Création d'une colonne `accident_date` calculée

### 🟡 Gold - Agrégations métier
Trois datasets prêts à consommer :

| Dataset | Description |
|---|---|
| `by_month` | Nombre d'accidents par mois en 2023 |
| `by_department` | Classement des départements les plus accidentogènes |
| `by_weather` | Répartition des accidents par condition météo |

---

## 🗺️ Roadmap

- [x] Phase 1 : Pipeline Python (Bronze / Silver / Gold)
- [ ] Phase 2 : Tests unitaires avec pytest
- [ ] Phase 3 : Orchestration avec Apache Airflow
- [ ] Phase 4 : Réécriture en Scala / Spark

---

## 🔗 Contexte

Ce projet est un projet portfolio personnel qui illustre les principes de l'architecture Medallion tels qu'appliqués en environnement professionnel.  
En parallèle, j'implémente cette même architecture en production chez **Skeepers** avec **Scala**, **Apache Iceberg** et **AWS S3**.