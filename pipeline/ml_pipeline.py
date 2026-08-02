"""
Pipeline ML — BGL Replay
1. Extraction des features depuis Redis (logs + métriques)
2. One-Class SVM pour détection d'anomalies (non supervisé)
3. K-Means pour clustering des anomalies détectées
4. Évaluation vs ground truth BGL
5. Stockage des résultats dans Redis pour le LLM
"""

import redis
import json
import numpy as np
import pandas as pd
from datetime import datetime
from collections import defaultdict
from sklearn.svm import OneClassSVM
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, confusion_matrix
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

print("=" * 60)
print("PIPELINE ML — One-Class SVM + K-Means")
print("=" * 60)

# -------------------------------------------------------
# ÉTAPE 1 — Charger les données depuis Redis
# -------------------------------------------------------
print("\n[1/5] Chargement des données depuis Redis...")

nb_logs = r.llen("bgl_logs_all")
nb_metriques = r.llen("bgl_metriques")
print(f"  Logs BGL total  : {nb_logs}")
print(f"  Métriques total : {nb_metriques}")

if nb_logs < 50:
    print("  ERREUR : pas assez de logs. Lance d'abord collector_redis.py")
    exit(1)

logs_raw = []
for i in range(min(nb_logs, 500)):
    log_json = r.lindex("bgl_logs_all", i)
    if log_json:
        logs_raw.append(json.loads(log_json))

metriques_raw = []
for i in range(min(nb_metriques, 500)):
    m_json = r.lindex("bgl_metriques", i)
    if m_json:
        metriques_raw.append(json.loads(m_json))

nb_normaux_gt = sum(1 for l in logs_raw if not l.get("is_anomaly", False))
nb_anomalies_gt = sum(1 for l in logs_raw if l.get("is_anomaly", False))
print(f"  Normaux (ground truth)   : {nb_normaux_gt}")
print(f"  Anomalies (ground truth) : {nb_anomalies_gt}")
print(f"  Ratio anomalies          : {nb_anomalies_gt/len(logs_raw)*100:.1f}%")

# -------------------------------------------------------
# ÉTAPE 2 — Parser les logs avec Drain + vectoriser
# -------------------------------------------------------
print("\n[2/5] Parsing Drain + vectorisation des features...")

config = TemplateMinerConfig()
config.drain_depth = 4
config.drain_sim_th = 0.5
config.drain_max_children = 100
miner = TemplateMiner(config=config)

# Parser tous les logs
templates_count = defaultdict(int)
components_set = set()
levels_set = set()

for log in logs_raw:
    content = log.get("content", "")
    if content:
        result = miner.add_log_message(content)
        log["template"] = result["template_mined"]
        log["cluster_id"] = result["cluster_id"]
        templates_count[log["template"]] += 1
    components_set.add(log.get("component", "UNKNOWN"))
    levels_set.add(log.get("level", "INFO"))

all_templates = list(templates_count.keys())
all_components = list(components_set)
all_levels = list(levels_set)

print(f"  Templates Drain    : {len(all_templates)}")
print(f"  Composants uniques : {len(all_components)}")

# Vectoriser chaque log en features numériques
def log_to_features(log):
    template = log.get("template", "")
    component = log.get("component", "UNKNOWN")
    level = log.get("level", "INFO")
    cluster_id = log.get("cluster_id", 0)

    t_idx = all_templates.index(template) if template in all_templates else 0
    c_idx = all_components.index(component) if component in all_components else 0
    l_idx = all_levels.index(level) if level in all_levels else 0

    # Fréquence du template dans le dataset
    template_freq = templates_count.get(template, 0) / max(len(logs_raw), 1)

    # Level encodé : FATAL=3, ERROR=2, WARNING=1, INFO=0
    level_score = {"FATAL": 3, "ERROR": 2, "WARNING": 1, "WARN": 1, "INFO": 0}.get(level, 0)

    return [t_idx, c_idx, cluster_id, template_freq, level_score, l_idx]

feature_names = ["template_id", "component_id", "drain_cluster",
                 "template_freq", "level_score", "level_id"]

X_logs = np.array([log_to_features(l) for l in logs_raw])
y_true = np.array([1 if l.get("is_anomaly", False) else 0 for l in logs_raw])

print(f"  Features extraites : {X_logs.shape}")

# -------------------------------------------------------
# ÉTAPE 3 — One-Class SVM
# -------------------------------------------------------
print("\n[3/5] One-Class SVM (entraîné sur les logs normaux uniquement)...")

scaler = StandardScaler()

# Entraîner UNIQUEMENT sur les logs normaux (c'est le principe du One-Class SVM)
X_normaux = X_logs[y_true == 0]
X_all_scaled = scaler.fit_transform(X_logs)
X_normaux_scaled = scaler.transform(X_normaux)

print(f"  Entraînement sur {len(X_normaux)} logs normaux...")

# nu = pourcentage estimé d'anomalies dans les données d'entraînement
# On met 0.05 car on s'attend à peu d'anomalies dans les données normales
svm = OneClassSVM(
    kernel="rbf",
    gamma="scale",
    nu=0.05
)
svm.fit(X_normaux_scaled)

# Prédire sur TOUS les logs
predictions_svm = svm.predict(X_all_scaled)
scores_svm = svm.decision_function(X_all_scaled)

# One-Class SVM : +1 = normal, -1 = anomalie
y_pred = (predictions_svm == -1).astype(int)

anomalies_idx = np.where(y_pred == 1)[0]
normaux_idx = np.where(y_pred == 0)[0]

print(f"  Résultats SVM :")
print(f"    Normaux détectés   : {len(normaux_idx)}")
print(f"    Anomalies détectées: {len(anomalies_idx)}")
print(f"    Taux anomalies     : {len(anomalies_idx)/len(y_pred)*100:.1f}%")

# Évaluation vs ground truth
print(f"\n  Évaluation vs ground truth BGL :")
tp = int(np.sum((y_pred == 1) & (y_true == 1)))
tn = int(np.sum((y_pred == 0) & (y_true == 0)))
fp = int(np.sum((y_pred == 1) & (y_true == 0)))
fn = int(np.sum((y_pred == 0) & (y_true == 1)))

precision = tp / max(tp + fp, 1)
recall = tp / max(tp + fn, 1)
f1 = 2 * precision * recall / max(precision + recall, 1e-6)

print(f"    TP (vrais positifs) : {tp}")
print(f"    TN (vrais négatifs) : {tn}")
print(f"    FP (faux positifs)  : {fp}")
print(f"    FN (faux négatifs)  : {fn}")
print(f"    Précision           : {precision:.3f}")
print(f"    Rappel              : {recall:.3f}")
print(f"    F1-Score            : {f1:.3f}")

# -------------------------------------------------------
# ÉTAPE 4 — K-Means sur les anomalies détectées
# -------------------------------------------------------
print("\n[4/5] K-Means clustering sur les anomalies détectées...")

# 12 clusters = 12 familles BGL
BGL_LABELS = ["KERNDTLB", "KERNSTORE", "KERNMNTF", "KERNSTOR",
              "APPREAD", "APPWRITE", "APPSEV", "APPCHILD",
              "HARDWARE", "NETWORK", "MEMORY", "OTHER"]

clusters_results = []

if len(anomalies_idx) >= 12:
    X_anomalies = X_all_scaled[anomalies_idx]
    n_clusters = min(12, len(anomalies_idx))

    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    cluster_labels = kmeans.fit_predict(X_anomalies)

    print(f"  Clusters formés : {n_clusters}")

    for cid in range(n_clusters):
        mask = cluster_labels == cid
        logs_in_cluster = [logs_raw[anomalies_idx[i]]
                           for i in range(len(anomalies_idx)) if mask[i]]

        if not logs_in_cluster:
            continue

        # Label BGL dominant dans ce cluster
        bgl_labels_in_cluster = defaultdict(int)
        components_in_cluster = defaultdict(int)
        levels_in_cluster = defaultdict(int)

        for log in logs_in_cluster:
            bgl_labels_in_cluster[log.get("label", "-")] += 1
            components_in_cluster[log.get("component", "?")] += 1
            levels_in_cluster[log.get("level", "?")] += 1

        label_dominant = max(bgl_labels_in_cluster, key=bgl_labels_in_cluster.get)
        component_dominant = max(components_in_cluster, key=components_in_cluster.get)

        # Score moyen SVM pour ce cluster (plus négatif = plus anormal)
        scores_cluster = scores_svm[anomalies_idx[mask]]
        score_moyen = float(scores_cluster.mean())

        # Top templates dans ce cluster
        templates_in_cluster = defaultdict(int)
        for log in logs_in_cluster:
            templates_in_cluster[log.get("template", "?")] += 1
        top_template = max(templates_in_cluster, key=templates_in_cluster.get)

        cluster_info = {
            "cluster_id": cid,
            "famille_bgl_dominante": label_dominant,
            "composant_dominant": component_dominant,
            "nb_logs": len(logs_in_cluster),
            "score_svm_moyen": round(score_moyen, 4),
            "bgl_labels": dict(bgl_labels_in_cluster),
            "top_template": top_template[:100],
            "severite": "CRITIQUE" if score_moyen < -0.5 else "ÉLEVÉE" if score_moyen < -0.2 else "MODÉRÉE"
        }
        clusters_results.append(cluster_info)

        print(f"    Cluster {cid:2d} | {label_dominant:12s} | "
              f"{len(logs_in_cluster):3d} logs | "
              f"Score SVM: {score_moyen:+.3f} | "
              f"{cluster_info['severite']}")

elif len(anomalies_idx) > 0:
    print(f"  Seulement {len(anomalies_idx)} anomalies — pas de clustering")
    for idx in anomalies_idx:
        log = logs_raw[idx]
        clusters_results.append({
            "cluster_id": 0,
            "famille_bgl_dominante": log.get("label", "-"),
            "composant_dominant": log.get("component", "?"),
            "nb_logs": 1,
            "score_svm_moyen": round(float(scores_svm[idx]), 4),
            "bgl_labels": {log.get("label", "-"): 1},
            "top_template": log.get("template", "?")[:100],
            "severite": "MODÉRÉE"
        })
else:
    print("  Aucune anomalie détectée — système normal")

# -------------------------------------------------------
# ÉTAPE 5 — Stocker les résultats dans Redis pour le LLM
# -------------------------------------------------------
print("\n[5/5] Stockage des résultats dans Redis...")

# Top templates des anomalies
anomalies_templates = defaultdict(int)
for idx in anomalies_idx:
    template = logs_raw[idx].get("template", "?")
    anomalies_templates[template] += 1

top_templates_anomalies = dict(sorted(
    anomalies_templates.items(),
    key=lambda x: x[1], reverse=True
)[:5])

# Features les plus déviantes
features_deviation = {}
if len(anomalies_idx) > 0 and len(normaux_idx) > 0:
    X_anom = X_logs[anomalies_idx]
    X_norm = X_logs[normaux_idx]
    for i, feat in enumerate(feature_names):
        mean_anom = X_anom[:, i].mean()
        mean_norm = X_norm[:, i].mean()
        features_deviation[feat] = round(float(mean_anom - mean_norm), 4)

# Résumé structuré pour le LLM
resultats_ml = {
    "timestamp": datetime.utcnow().isoformat(),
    "resume": {
        "total_logs_analyses": len(logs_raw),
        "normaux_detectes": int(len(normaux_idx)),
        "anomalies_detectees": int(len(anomalies_idx)),
        "taux_anomalie_pct": round(len(anomalies_idx) / len(logs_raw) * 100, 2),
        "ground_truth_anomalies": int(nb_anomalies_gt),
        "precision": round(precision, 3),
        "rappel": round(recall, 3),
        "f1_score": round(f1, 3)
    },
    "clusters": clusters_results,
    "top_templates_anomalies": top_templates_anomalies,
    "features_deviation": features_deviation,
    "metriques_systeme": metriques_raw[0] if metriques_raw else {}
}

r.set("ml_results", json.dumps(resultats_ml))
print(f"  Résultats stockés dans Redis : 'ml_results'")

# Afficher le résumé final
print("\n" + "=" * 60)
print("RÉSUMÉ FINAL")
print("=" * 60)
print(f"  Logs analysés    : {len(logs_raw)}")
print(f"  Anomalies SVM    : {len(anomalies_idx)} ({len(anomalies_idx)/len(logs_raw)*100:.1f}%)")
print(f"  Clusters formés  : {len(clusters_results)}")
print(f"  F1-Score         : {f1:.3f}")
print(f"\n  Redis key 'ml_results' prête pour le LLM")
print("=" * 60)
