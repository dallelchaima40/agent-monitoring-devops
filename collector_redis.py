import redis
import json
import time
import requests
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta

# Connexions
es = Elasticsearch(
    "http://localhost:9200",
    headers={"Accept": "application/json", "Content-Type": "application/json"}
)
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

print("Démarrage du collecteur Redis BGL...")
print(f"Elasticsearch : {'OK' if es.ping() else 'ERREUR'}")
print(f"Redis         : {'OK' if r.ping() else 'ERREUR'}")
print("-" * 50)

def collecter_logs_bgl():
    """Lit les logs BGL depuis Elasticsearch et les stocke dans Redis"""
    try:
        maintenant = datetime.utcnow()
        il_y_a_2_min = maintenant - timedelta(minutes=30)

        result = es.search(index="bgl-replay-*", body={
            "size": 100,
            "sort": [{"@timestamp": {"order": "desc"}}],
            "query": {
                "bool": {
                    "must": [
                        {"term": {"is_anomaly": "true"}},
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": il_y_a_2_min.isoformat(),
                                    "lte": maintenant.isoformat()
                                }
                            }
                        }
                    ]
                }
            }
        })

        logs = result['hits']['hits']
        compteur = 0

        for log in logs:
            source = log['_source']
            entree = {
                "type": "bgl_log",
                "timestamp": source.get('@timestamp', ''),
                "label": source.get('bgl_label', '-'),
                "component": source.get('bgl_component', ''),
                "level": source.get('bgl_level', ''),
                "content": source.get('bgl_content', ''),
                "is_anomaly": source.get('is_anomaly', 'false'),
                "anomaly_label": source.get('anomaly_label', ''),
                "collecte_a": datetime.utcnow().isoformat()
            }
            r.lpush("bgl_logs_anomalies", json.dumps(entree))
            compteur += 1

        r.ltrim("bgl_logs_anomalies", 0, 999)

        if compteur > 0:
            print(f"[LOGS BGL] {compteur} anomalies stockées dans Redis")

    except Exception as e:
        print(f"[LOGS BGL] Erreur : {e}")


def collecter_metriques_prometheus():
    """Lit les métriques depuis Prometheus et les stocke dans Redis"""
    try:
        base_url = "http://localhost:9090/api/v1/query"

        # Débit de logs BGL
        debit_resp = requests.get(base_url, params={
            "query": "bgl_replayer_lines_per_second"
        })
        debit_data = debit_resp.json()

        # Taux d'anomalies
        anomalies_resp = requests.get(base_url, params={
            "query": "increase(bgl_replayer_anomalies_injected_total[5m])"
        })
        anomalies_data = anomalies_resp.json()

        # CPU iterations
        cpu_resp = requests.get(base_url, params={
            "query": "rate(bgl_replayer_cpu_iterations_total[1m])"
        })
        cpu_data = cpu_resp.json()

        # RAM node
        ram_resp = requests.get(base_url, params={
            "query": "100 * (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))"
        })
        ram_data = ram_resp.json()

        debit_val = 0
        anomalies_val = 0
        cpu_val = 0
        ram_val = 0

        if debit_data['data']['result']:
            debit_val = float(debit_data['data']['result'][0]['value'][1])

        if anomalies_data['data']['result']:
            for r_item in anomalies_data['data']['result']:
                anomalies_val += float(r_item['value'][1])

        if cpu_data['data']['result']:
            cpu_val = float(cpu_data['data']['result'][0]['value'][1])

        if ram_data['data']['result']:
            ram_val = float(ram_data['data']['result'][0]['value'][1])

        entree = {
            "type": "metrique",
            "timestamp": datetime.utcnow().isoformat(),
            "debit_logs_par_sec": round(debit_val, 4),
            "taux_anomalies_par_sec": round(anomalies_val, 4),
            "cpu_iterations_par_sec": round(cpu_val, 2),
            "ram_pct": round(ram_val, 2),
            "collecte_a": datetime.utcnow().isoformat()
        }

        r.lpush("bgl_metriques", json.dumps(entree))
        r.ltrim("bgl_metriques", 0, 999)

        print(
            f"[METRIQUES] Débit: {debit_val:.2f} logs/sec | "
            f"Anomalies: {anomalies_val:.4f}/sec | "
            f"RAM: {ram_val:.1f}%"
        )

    except Exception as e:
        print(f"[METRIQUES] Erreur : {e}")


def afficher_stats_redis():
    nb_logs = r.llen("bgl_logs_anomalies")
    nb_metriques = r.llen("bgl_metriques")
    print(f"[REDIS] Anomalies BGL: {nb_logs} | Métriques: {nb_metriques}")


# Boucle principale
print("Collecte toutes les 30 secondes... Ctrl+C pour arrêter")
print("-" * 50)

while True:
    collecter_logs_bgl()
    collecter_metriques_prometheus()
    afficher_stats_redis()
    print("-" * 50)
    time.sleep(30)
