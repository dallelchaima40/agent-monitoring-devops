"""
Pipeline IA v2 — BGL Replay
1. LOF sur les features extraites (détection anomalies non supervisée)
2. K-Means clustering (12 clusters = 12 familles BGL)
3. LLM sur résumé structuré uniquement
"""

import redis
import json
import numpy as np
import pandas as pd
from datetime import datetime
from collections import defaultdict
from sklearn.neighbors import LocalOutlierFactor
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

# --- Connexion Redis ---
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

print("=" * 60)
print("PIPELINE IA v2 — BGL Replay")
print("=" * 60)

# -------------------------------------------------------
# ÉTAPE 1 — Charger les données depuis Redis
# -------------------------------------------------------
print("\n[1/4] Chargement des données depuis Redis...")

nb_logs = r.llen("bgl_logs_anomalies")
nb_metriques = r.llen("bgl_metriques")
print(f"  Logs BGL anomalies : {nb_logs}")
print(f"  Métriques          : {nb_metriques}")

logs_raw = []
for i in range(min(nb_logs, 1000)):
    log_json = r.lindex("bgl_logs_anomalies", i)
    if log_json:
        logs_raw.append(json.loads(log_json))

metriques_raw = []
for i in range(nb_metriques):
    m_json = r.lindex("bgl_metriques", i)
    if m_json:
        metriques_raw.append(json.loads(m_json))

# -------------------------------------------------------
# ÉTAPE 2 — Parser les logs avec Drain + extraire features
# -------------------------------------------------------
print("\n[2/4] Parsing Drain + extraction de features...")

config = TemplateMinerConfig()
config.drain_depth = 4
config.drain_sim_th = 0.5
config.drain_max_children = 100
miner = TemplateMiner(config=config)

# 12 vraies catégories BGL
BGL_CATEGORIES = [
    "KERNDTLB", "KERNSTORE", "KERNMNTF", "KERNSTOR",
    "APPREAD", "APPWRITE", "APPSEV", "APPCHILD",
    "HARDWARE", "NETWORK", "MEMORY", "OTHER"
]

templates_count = defaultdict(int)
labels_count = defaultdict(int)
components_count = defaultdict(int)
logs_parses = []

for log in logs_raw:
    content = log.get("content", "")
    label = log.get("label", "-")
    component = log.get("component", "UNKNOWN")

    if not content:
        continue

    result = miner.add_log_message(content)
    template = result["template_mined"]
    cluster_id = result["cluster_id"]

    templates_count[template] += 1
    labels_count[label] += 1
    components_count[component] += 1

    logs_parses.append({
        **log,
        "template": template,
        "cluster_id": cluster_id
    })

print(f"  Logs parsés        : {len(logs_parses)}")
print(f"  Templates uniques  : {len(templates_count)}")
print(f"  Labels BGL         :")
for label, count in sorted(labels_count.items(),
                            key=lambda x: x[1], reverse=True):
    print(f"    [{count:4d}x] {label}")

# -------------------------------------------------------
# ÉTAPE 3 — LOF sur les métriques
# -------------------------------------------------------
print("\n[3/4] Détection d'anomalies avec LOF...")

if len(metriques_raw) < 5:
    print("  Pas assez de métriques. Lance d'abord collector_redis.py")
    lof_results = {}
else:
    df = pd.DataFrame(metriques_raw)
    features = ["debit_logs_par_sec", "ram_pct"]
    if "cpu_iterations_par_sec" in df.columns:
        features.append("cpu_iterations_par_sec")
    if "taux_anomalies_par_sec" in df.columns:
        features.append("taux_anomalies_par_sec")

    X = df[features].fillna(0).values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # LOF — n_neighbors adapté à la taille du dataset
    n_neighbors = min(5, len(X) - 1)
    lof = LocalOutlierFactor(
        n_neighbors=n_neighbors,
        contamination=0.2,
        novelty=False
    )
    predictions = lof.fit_predict(X_scaled)
    scores_lof = lof.negative_outlier_factor_

    anomalies_idx = np.where(predictions == -1)[0]
    normaux_idx = np.where(predictions == 1)[0]

    print(f"  Total points      : {len(X)}")
    print(f"  Points normaux    : {len(normaux_idx)}")
    print(f"  Anomalies LOF     : {len(anomalies_idx)}")

    # Features les plus déviantes pour chaque anomalie
    anomalies_details = []
    for idx in anomalies_idx:
        point = X[idx]
        point_scaled = X_scaled[idx]
        
        # Feature la plus déviante = celle dont la valeur absolue normalisée est max
        feat_idx_max = np.argmax(np.abs(point_scaled))
        feat_principale = features[feat_idx_max]
        val_principale = point[feat_idx_max]
        
        # Comparer à la moyenne normale
        mean_normal = X[normaux_idx][:, feat_idx_max].mean() if len(normaux_idx) > 0 else 0
        deviation = val_principale - mean_normal

        anomalies_details.append({
            "index": int(idx),
            "score_lof": round(float(scores_lof[idx]), 3),
            "features": {f: round(float(point[i]), 3) for i, f in enumerate(features)},
            "feature_principale": feat_principale,
            "valeur_principale": round(float(val_principale), 3),
            "deviation_vs_normal": round(float(deviation), 3)
        })

    anomalies_details.sort(key=lambda x: x["score_lof"])

    if anomalies_details:
        print(f"\n  Top anomalies (score LOF le plus négatif = le plus anormal) :")
        for a in anomalies_details[:3]:
            print(f"    Score LOF: {a['score_lof']:.3f} | "
                  f"Feature: {a['feature_principale']} = {a['valeur_principale']:.3f} | "
                  f"Déviation: {a['deviation_vs_normal']:+.3f}")

    lof_results = {
        "n_points": len(X),
        "n_anomalies": len(anomalies_idx),
        "n_normaux": len(normaux_idx),
        "features_utilisees": features,
        "anomalies": anomalies_details[:5]
    }

# -------------------------------------------------------
# ÉTAPE 4 — K-Means clustering (12 clusters = 12 familles)
# -------------------------------------------------------
print("\n[4/4] Clustering K-Means (12 familles BGL)...")

if len(logs_parses) >= 12:
    # Vectoriser les logs par template et composant
    all_templates = list(templates_count.keys())
    all_components = list(components_count.keys())

    # Feature vector par log : [template_id, component_id]
    def log_to_vector(log):
        template = log.get("template", "")
        component = log.get("component", "UNKNOWN")
        t_idx = all_templates.index(template) if template in all_templates else 0
        c_idx = all_components.index(component) if component in all_components else 0
        return [t_idx, c_idx]

    X_logs = np.array([log_to_vector(l) for l in logs_parses])

    kmeans = KMeans(n_clusters=12, random_state=42, n_init=10)
    cluster_labels = kmeans.fit_predict(X_logs)

    # Associer chaque cluster au label BGL dominant
    cluster_to_bgl = {}
    for cluster_id in range(12):
        mask = cluster_labels == cluster_id
        logs_in_cluster = [logs_parses[i] for i in range(len(logs_parses)) if mask[i]]
        
        if not logs_in_cluster:
            cluster_to_bgl[cluster_id] = {"famille": "UNKNOWN", "count": 0, "labels": {}}
            continue

        labels_in_cluster = defaultdict(int)
        for log in logs_in_cluster:
            labels_in_cluster[log.get("label", "-")] += 1

        label_dominant = max(labels_in_cluster, key=labels_in_cluster.get)
        cluster_to_bgl[cluster_id] = {
            "famille": label_dominant,
            "count": len(logs_in_cluster),
            "labels": dict(labels_in_cluster)
        }

    print(f"  Clusters découverts :")
    for cid, info in sorted(cluster_to_bgl.items(),
                             key=lambda x: x[1]["count"], reverse=True):
        if info["count"] > 0:
            print(f"    Cluster {cid:2d} → {info['famille']:12s} ({info['count']} logs)")

    kmeans_results = {
        "n_clusters": 12,
        "clusters": cluster_to_bgl
    }
else:
    print(f"  Pas assez de logs ({len(logs_parses)}). Minimum 12 requis.")
    kmeans_results = {}

# -------------------------------------------------------
# Stocker les résultats dans Redis
# -------------------------------------------------------
resultats = {
    "timestamp": datetime.utcnow().isoformat(),
    "lof": lof_results,
    "kmeans": kmeans_results,
    "drain": {
        "n_logs_analyses": len(logs_parses),
        "n_templates": len(templates_count),
        "top_templates": dict(list(sorted(
            templates_count.items(),
            key=lambda x: x[1], reverse=True
        ))[:5]),
        "labels_bgl": dict(labels_count),
        "components": dict(components_count)
    }
}

r.set("bgl_ai_v2_results", json.dumps(resultats))
print(f"\nRésultats stockés dans Redis : 'bgl_ai_v2_results'")

print("\n" + "=" * 60)
print("Pipeline IA v2 terminé !")
print("Prochaine étape : LLM sur résumé structuré")
print("=" * 60)
