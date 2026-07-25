import redis
import json
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
from datetime import datetime

# Connexion Redis
r = redis.Redis(host='redis', port=6379, decode_responses=True)

print("Démarrage du clustering K-means...")
print("-" * 50)

def charger_donnees():
    nb_metriques = r.llen("metriques")
    metriques_list = []
    for i in range(nb_metriques):
        m_json = r.lindex("metriques", i)
        if m_json:
            m = json.loads(m_json)
            metriques_list.append({
                "timestamp": m.get("timestamp", ""),
                "cpu_pct": float(m.get("cpu_pct", 0)),
                "ram_pct": float(m.get("ram_pct", 0)),
                "reseau_bytes_sec": float(m.get("reseau_bytes_sec", 0))
            })
    return pd.DataFrame(metriques_list)

def clustering_anomalies():
    df = charger_donnees()
    features = ["cpu_pct", "ram_pct", "reseau_bytes_sec"]
    X = df[features].values

    # Isoler les anomalies avec Isolation Forest
    iso = IsolationForest(n_estimators=100, contamination=0.1, random_state=42)
    predictions = iso.fit_predict(X)
    scores = iso.score_samples(X)

    anomalies_idx = np.where(predictions == -1)[0]
    X_anomalies = X[anomalies_idx]

    print(f"Total points      : {len(X)}")
    print(f"Anomalies trouvées : {len(X_anomalies)}")

    if len(X_anomalies) < 3:
        print("Pas assez d'anomalies pour le clustering.")
        return

    # Normaliser les données
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_anomalies)

    # Choisir le nombre de clusters
    n_clusters = min(3, len(X_anomalies))

    # Entraîner K-means
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(X_scaled)

    print(f"Nombre de clusters : {n_clusters}")
    print("-" * 50)

    # Analyser chaque cluster
    clusters_info = []
    for cluster_id in range(n_clusters):
        mask = clusters == cluster_id
        points_cluster = X_anomalies[mask]

        cpu_moy = points_cluster[:, 0].mean()
        ram_moy = points_cluster[:, 1].mean()
        reseau_moy = points_cluster[:, 2].mean()

        # Identifier le type d'anomalie
        if cpu_moy > 50:
            type_anomalie = "SURCHARGE CPU"
            emoji = "🔴"
        elif ram_moy > 80:
            type_anomalie = "SURCHARGE RAM"
            emoji = "🟠"
        elif reseau_moy > 100:
            type_anomalie = "SURCHARGE RÉSEAU"
            emoji = "🟡"
        elif cpu_moy > 20:
            type_anomalie = "CPU ÉLEVÉ"
            emoji = "🟡"
        else:
            type_anomalie = "ANOMALIE MIXTE"
            emoji = "🔵"

        print(f"\n  Cluster {cluster_id} — {emoji} {type_anomalie}")
        print(f"    Nombre de points : {mask.sum()}")
        print(f"    CPU moyen        : {cpu_moy:.1f}%")
        print(f"    RAM moyenne      : {ram_moy:.1f}%")
        print(f"    Réseau moyen     : {reseau_moy:.0f} bytes/sec")

        cluster_info = {
            "cluster_id": int(cluster_id),
            "type_anomalie": type_anomalie,
            "nb_points": int(mask.sum()),
            "cpu_moy": round(float(cpu_moy), 2),
            "ram_moy": round(float(ram_moy), 2),
            "reseau_moy": round(float(reseau_moy), 2),
            "timestamp": datetime.utcnow().isoformat()
        }
        clusters_info.append(cluster_info)

    # Stocker dans Redis
    r.set("clusters_anomalies", json.dumps(clusters_info))
    print(f"\nClusters stockés dans Redis sous la clé 'clusters_anomalies'")

    # Afficher un résumé
    print(f"\n{'='*50}")
    print("RÉSUMÉ DES PATTERNS D'ANOMALIES :")
    print('='*50)
    for c in clusters_info:
        print(f"  {c['type_anomalie']:20s} — {c['nb_points']} occurrences")

    return clusters_info

clusters = clustering_anomalies()

print(f"\n{'='*50}")
print("K-means terminé avec succès !")
print(f"{'='*50}")