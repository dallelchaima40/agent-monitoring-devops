import redis
import json
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from datetime import datetime
from collections import defaultdict

r = redis.Redis(host='redis', port=6379, decode_responses=True)

print("Démarrage du détecteur d'anomalies Isolation Forest...")
print("-" * 50)

def preparer_donnees():
    """Prépare les données depuis Redis pour Isolation Forest"""
    
    # Récupérer les métriques depuis Redis
    nb_metriques = r.llen("metriques")
    nb_logs = r.llen("logs_parsed")
    
    print(f"Métriques disponibles : {nb_metriques}")
    print(f"Logs parsés disponibles : {nb_logs}")
    
    if nb_metriques < 10:
        print("Pas assez de métriques. Attends que le collector.py collecte plus de données.")
        return None

    # Lire les métriques
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

    # Lire les logs parsés et compter les templates par période
    templates_count = defaultdict(int)
    for i in range(nb_logs):
        l_json = r.lindex("logs_parsed", i)
        if l_json:
            l = json.loads(l_json)
            template = l.get("template", "unknown")
            templates_count[template] += 1

    print(f"\nRépartition des templates :")
    for template, count in sorted(templates_count.items(),
                                   key=lambda x: x[1], reverse=True)[:5]:
        print(f"  [{count:3d}x] {template[:60]}")

    return metriques_list, dict(templates_count)


def entrainer_modele(metriques_list):
    """Entraîne le modèle Isolation Forest"""

    # Créer le DataFrame
    df = pd.DataFrame(metriques_list)

    # Features pour le modèle
    features = ["cpu_pct", "ram_pct", "reseau_bytes_sec"]
    X = df[features].values

    print(f"\nEntraînement sur {len(X)} points de données...")
    print(f"Features utilisées : {features}")
    print(f"\nStatistiques des données :")
    print(f"  CPU    — min: {X[:,0].min():.1f}% | max: {X[:,0].max():.1f}% | moy: {X[:,0].mean():.1f}%")
    print(f"  RAM    — min: {X[:,1].min():.1f}% | max: {X[:,1].max():.1f}% | moy: {X[:,1].mean():.1f}%")
    print(f"  Réseau — min: {X[:,2].min():.0f} | max: {X[:,2].max():.0f} | moy: {X[:,2].mean():.0f} bytes/sec")

    # Entraîner Isolation Forest
    # contamination=0.1 : on s'attend à 10% d'anomalies
    modele = IsolationForest(
        n_estimators=100,
        contamination=0.1,
        random_state=42
    )
    modele.fit(X)

    print(f"\nModèle entraîné avec succès !")
    return modele, df, features


def detecter_anomalies(modele, df, features):
    """Détecte les anomalies dans les données"""

    X = df[features].values
    predictions = modele.predict(X)
    scores = modele.score_samples(X)

    anomalies = df[predictions == -1].copy()
    normaux = df[predictions == 1].copy()

    print(f"\nRésultats de la détection :")
    print(f"  Points normaux   : {len(normaux)}")
    print(f"  Anomalies détectées : {len(anomalies)}")
    print(f"  Taux d'anomalies : {len(anomalies)/len(df)*100:.1f}%")

    if len(anomalies) > 0:
        print(f"\nDétail des anomalies détectées :")
        print("-" * 50)
        anomalies_avec_scores = anomalies.copy()
        anomalies_avec_scores['score'] = scores[predictions == -1]
        anomalies_triees = anomalies_avec_scores.sort_values('score')

        for _, row in anomalies_triees.head(10).iterrows():
            print(f"  ANOMALIE | CPU: {row['cpu_pct']:.1f}% | "
                  f"RAM: {row['ram_pct']:.1f}% | "
                  f"Réseau: {row['reseau_bytes_sec']:.0f} bytes/sec | "
                  f"Score: {row['score']:.3f}")

    return anomalies, scores


def stocker_resultats_redis(anomalies, scores, df, features):
    """Stocke les résultats dans Redis"""

    predictions = np.where(scores > np.percentile(scores, 10), 1, -1)

    resultats = {
        "timestamp": datetime.utcnow().isoformat(),
        "total_points": len(df),
        "nb_anomalies": len(anomalies),
        "taux_anomalie_pct": round(len(anomalies)/len(df)*100, 2),
        "anomalies": anomalies[features].to_dict('records') if len(anomalies) > 0 else []
    }

    r.set("derniere_analyse", json.dumps(resultats))
    r.lpush("historique_analyses", json.dumps(resultats))
    r.ltrim("historique_analyses", 0, 99)

    print(f"\nRésultats stockés dans Redis sous la clé 'derniere_analyse'")


def simuler_anomalie_et_detecter(modele, features):
    """Simule une anomalie et teste la détection"""

    print(f"\n{'='*50}")
    print("TEST — Simulation d'une anomalie CPU à 95%")
    print('='*50)

    # Données normales
    normal = np.array([[8.0, 30.0, 42.0]])
    score_normal = modele.score_samples(normal)[0]
    prediction_normal = modele.predict(normal)[0]

    print(f"\nSituation normale :")
    print(f"  CPU: 8% | RAM: 30% | Réseau: 42 bytes/sec")
    print(f"  Score: {score_normal:.3f} | "
          f"Résultat: {'NORMAL ✓' if prediction_normal == 1 else 'ANOMALIE ✗'}")

    # Anomalie CPU
    anomalie_cpu = np.array([[95.0, 30.0, 42.0]])
    score_anomalie = modele.score_samples(anomalie_cpu)[0]
    prediction_anomalie = modele.predict(anomalie_cpu)[0]

    print(f"\nSituation anormale (CPU à 95%) :")
    print(f"  CPU: 95% | RAM: 30% | Réseau: 42 bytes/sec")
    print(f"  Score: {score_anomalie:.3f} | "
          f"Résultat: {'NORMAL ✓' if prediction_anomalie == 1 else 'ANOMALIE DÉTECTÉE ! ✗'}")

    # Anomalie RAM
    anomalie_ram = np.array([[8.0, 95.0, 42.0]])
    score_ram = modele.score_samples(anomalie_ram)[0]
    prediction_ram = modele.predict(anomalie_ram)[0]

    print(f"\nSituation anormale (RAM à 95%) :")
    print(f"  CPU: 8% | RAM: 95% | Réseau: 42 bytes/sec")
    print(f"  Score: {score_ram:.3f} | "
          f"Résultat: {'NORMAL ✓' if prediction_ram == 1 else 'ANOMALIE DÉTECTÉE ! ✗'}")


# Programme principal
resultat = preparer_donnees()

if resultat:
    metriques_list, templates_count = resultat
    modele, df, features = entrainer_modele(metriques_list)
    anomalies, scores = detecter_anomalies(modele, df, features)
    stocker_resultats_redis(anomalies, scores, df, features)
    simuler_anomalie_et_detecter(modele, features)

    print(f"\n{'='*50}")
    print("Phase 2 IA terminée avec succès !")
    print("Le modèle Isolation Forest est entraîné.")
    print("Il peut maintenant détecter les anomalies en temps réel.")
    print('='*50)
