import redis
import json
import numpy as np
import pandas as pd
import shap
from sklearn.ensemble import IsolationForest
from datetime import datetime

# Connexion Redis
r = redis.Redis(host='redis', port=6379, decode_responses=True)

print("Démarrage de l'explicateur SHAP...")
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

def entrainer_et_expliquer():
    df = charger_donnees()
    features = ["cpu_pct", "ram_pct", "reseau_bytes_sec"]
    X = df[features].values

    # Entraîner Isolation Forest
    modele = IsolationForest(
        n_estimators=100,
        contamination=0.1,
        random_state=42
    )
    modele.fit(X)
    predictions = modele.predict(X)

    # Identifier les anomalies
    df['anomalie'] = predictions
    anomalies = df[predictions == -1]

    print(f"Total points      : {len(df)}")
    print(f"Anomalies trouvées : {len(anomalies)}")
    print("-" * 50)

    # Expliquer avec SHAP
    explainer = shap.TreeExplainer(modele)
    shap_values = explainer.shap_values(X)

    print("\nImportance globale des features (SHAP) :")
    print("-" * 50)
    importance = np.abs(shap_values).mean(axis=0)
    for feat, imp in sorted(zip(features, importance),
                            key=lambda x: x[1], reverse=True):
        barre = "█" * int(imp * 100)
        print(f"  {feat:20s} : {imp:.4f} {barre}")

    # Expliquer chaque anomalie
    print(f"\nExplication des anomalies :")
    print("-" * 50)

    anomalies_expliquees = []
    indices_anomalies = np.where(predictions == -1)[0]

    for idx in indices_anomalies[:5]:
        point = X[idx]
        shap_point = shap_values[idx]

        contributions = sorted(zip(features, shap_point),
                              key=lambda x: abs(x[1]), reverse=True)

        cause_principale = contributions[0][0]
        valeur_principale = point[features.index(cause_principale)]

        print(f"\n  ANOMALIE #{idx} :")
        print(f"    CPU: {point[0]:.1f}% | RAM: {point[1]:.1f}% | Réseau: {point[2]:.0f} bytes/sec")
        print(f"    Cause principale : {cause_principale} = {valeur_principale:.1f}")
        print(f"    Contributions SHAP :")
        for feat, contrib in contributions:
            direction = "↑ anormal" if contrib < 0 else "↓ normal"
            print(f"      {feat:20s} : {contrib:+.4f} {direction}")

        anomalie_expliquee = {
            "index": int(idx),
            "cpu_pct": float(point[0]),
            "ram_pct": float(point[1]),
            "reseau_bytes_sec": float(point[2]),
            "cause_principale": cause_principale,
            "valeur_principale": float(valeur_principale),
            "shap_contributions": {f: float(v) for f, v in zip(features, shap_point)},
            "timestamp": datetime.utcnow().isoformat()
        }
        anomalies_expliquees.append(anomalie_expliquee)

    # Stocker dans Redis
    r.set("anomalies_expliquees", json.dumps(anomalies_expliquees))
    print(f"\nAnomalies expliquées stockées dans Redis")

    return anomalies_expliquees, importance, features

anomalies_expliquees, importance, features = entrainer_et_expliquer()

print(f"\n{'='*50}")
print("SHAP terminé avec succès !")
print(f"{'='*50}")
