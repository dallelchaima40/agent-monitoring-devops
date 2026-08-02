# 🤖 Agent Monitoring DevOps IA

Un agent DevOps autonome basé sur **LangGraph** qui surveille une infrastructure en temps réel, détecte les anomalies par Machine Learning et génère des rapports d'analyse via **Gemini LLM**.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Agent LangGraph                          │
│                                                              │
│  infra_check → collect_data → ml_pipeline → llm_analyzer   │
│       ↓              ↓             ↓              ↓          │
│  [ERREUR]→END   [ERREUR]→ALERTE  [ERREUR]→ALERTE  ↓          │
│                                              decide_severite  │
│                                              ↙           ↘   │
│                                         send_alert    log_only│
└─────────────────────────────────────────────────────────────┘
```

### Stack technique

| Composant | Rôle |
|-----------|------|
| **Elasticsearch** | Stockage et indexation des logs BGL |
| **Logstash + Filebeat** | Collecte et parsing des logs |
| **Redis** | Bus de données entre les modules |
| **Prometheus + Node Exporter** | Métriques système |
| **Grafana** | Dashboards de visualisation |
| **cAdvisor** | Métriques conteneurs Docker |
| **log-replayer** | Simulation du trafic BGL avec injections d'anomalies |
| **One-Class SVM** | Détection d'anomalies non supervisée |
| **K-Means** | Clustering des anomalies en familles |
| **Gemini Flash** | Analyse LLM et génération de rapports |

## 📁 Structure du projet

```
agent-monitoring-devops/
├── agent/                         # Agent LangGraph (point d'entrée)
│   └── agent.py                   # Pipeline LangGraph 6 nœuds
│
├── pipeline/                      # Modules ML & LLM
│   ├── collector_redis.py         # Collecte ES → Redis
│   ├── ml_pipeline.py             # One-Class SVM + K-Means
│   ├── llm_analyzer_bgl.py        # Analyse Gemini → rapport
│   └── extract_features.py        # Extraction features ES+Prometheus → CSV
│
├── infra/                         # Configuration Docker
│   ├── filebeat/                  # Config Filebeat
│   ├── grafana/                   # Dashboards et datasources Grafana
│   ├── logstash/                  # Pipeline et config Logstash
│   ├── prometheus/                # Config Prometheus
│   └── log-replayer/              # Service de replay BGL (Dockerfile + replayer.py)
│
├── data/                          # Données source
│   ├── BGL.log                    # Dataset BGL (supercomputer BlueGene/L)
│   └── dataset_bgl_replay.csv     # Features extraites (généré)
│
├── rapports/                      # Rapports LLM générés (ignorés par Git)
│
├── docker-compose.yml             # Tous les services Docker
├── start.sh                       # Script de démarrage complet
├── .env.example                   # Template des variables d'environnement
└── README.md                      # Ce fichier
```

## 🚀 Installation et démarrage

### Prérequis
- Docker & Docker Compose
- Python 3.9+
- GitHub Codespaces (recommandé) ou Linux

### 1. Configurer l'environnement

```bash
cp .env.example .env
# Éditer .env et renseigner GEMINI_API_KEY
```

### 2. Installer les dépendances Python

```bash
pip install -r requirements.txt
```

> ⚠️ Utiliser `elasticsearch>=8.13,<9` — la version 9.x est incompatible avec le serveur Elasticsearch 8.13.4 du `docker-compose.yml`.

### 3. Démarrer l'infrastructure

```bash
bash start.sh
```

Ce script :
1. Configure `vm.max_map_count` pour Elasticsearch
2. Lance tous les conteneurs Docker
3. Attend que les services soient prêts
4. Expose les ports (GitHub Codespaces)

### 3. Lancer la collecte de données

```bash
# Dans un terminal séparé
python3 pipeline/collector_redis.py
```

### 4. Lancer l'agent

```bash
python3 agent/agent.py
```

## 🔄 Pipeline de l'agent

Le pipeline LangGraph exécute 6 nœuds en séquence :

1. **`infra_check`** — Vérifie que tous les services répondent (ES, Redis, Replayer, Prometheus)
2. **`collect_data`** — Lance `collector_redis.py` si les données Redis sont insuffisantes
3. **`run_ml_pipeline`** — Exécute One-Class SVM + K-Means sur les logs
4. **`run_llm_analyzer`** — Génère un rapport via Gemini Flash
5. **`decide_severite`** — Calcule la sévérité (CRITIQUE / ÉLEVÉ / MODÉRÉ / FAIBLE)
6. **`send_alert`** ou **`log_only`** — Envoie une alerte Redis ou log simple

## 🧠 Pipeline ML

| Étape | Algorithme | Rôle |
|-------|-----------|------|
| Parsing | Drain3 | Extraction de templates de logs |
| Détection | One-Class SVM (nu=0.05, kernel RBF) | Détection d'anomalies non supervisée |
| Clustering | K-Means (12 clusters) | Regroupement par famille BGL |
| Évaluation | F1-Score vs ground truth BGL | Mesure de qualité du modèle |

## 📊 Accès aux services (GitHub Codespaces)

| Service | URL |
|---------|-----|
| Grafana | `https://$CODESPACE_NAME-3000.app.github.dev` (admin/admin) |
| Kibana | `https://$CODESPACE_NAME-5601.app.github.dev` |
| Prometheus | `https://$CODESPACE_NAME-9090.app.github.dev` |
| cAdvisor | `https://$CODESPACE_NAME-8080.app.github.dev` |
| Replayer metrics | `https://$CODESPACE_NAME-8000.app.github.dev/metrics` |
| RedisInsight | `https://$CODESPACE_NAME-5540.app.github.dev` |

## 🗂️ Clés Redis utilisées

| Clé | Type | Contenu |
|-----|------|---------|
| `bgl_logs_all` | List | 500 derniers logs BGL collectés |
| `bgl_metriques` | List | Métriques Prometheus |
| `ml_results` | String | Résultats JSON du pipeline ML |
| `rapport_llm_bgl` | String | Dernier rapport Gemini |
| `alertes` | List | 100 dernières alertes (sévérité CRITIQUE/ÉLEVÉ) |
| `logs_monitoring` | List | Logs d'activité normale |
| `agent_last_run` | String | Résumé de la dernière exécution |

## 🔧 Commandes utiles

```bash
# Voir les logs du replayer
docker compose logs -f log-replayer

# Vérifier Redis
docker exec -it redis redis-cli

# Extraire les features pour le ML
python3 pipeline/extract_features.py --output data/dataset_bgl_replay.csv

# Voir les anomalies rejouées
docker compose logs log-replayer | grep Anomalie
```

## 📄 Licence

Projet académique — Dataset BGL : BlueGene/L Supercomputer Logs (Usenix CFDR).
