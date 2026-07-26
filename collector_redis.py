import redis
import json
import time
import requests
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta

es = Elasticsearch("http://localhost:9200")
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

print("Démarrage du collecteur Redis BGL...")
print(f"Elasticsearch : {'OK' if es.ping() else 'ERREUR'}")
print(f"Redis         : {'OK' if r.ping() else 'ERREUR'}")
print("-" * 50)

def collecter_logs_bgl():
    """Lit TOUS les logs BGL depuis Elasticsearch — normaux ET anomalies"""
    try:
        maintenant = datetime.utcnow()
        il_y_a_30_min = maintenant - timedelta(minutes=30)

        result = es.search(index="bgl-replay-*", body={
            "size": 200,
            "sort": [{"@timestamp": {"order": "desc"}}],
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": il_y_a_30_min.isoformat(),
                        "lte": maintenant.isoformat()
                    }
                }
            }
        })

        logs = result['hits']['hits']
        compteur_total = 0
        compteur_anomalies = 0
        compteur_normaux = 0

        for log in logs:
            source = log['_source']

            # is_anomaly vient de Logstash qui a lu le Label BGL
            # "-" = normal, tout autre label = anomalie réelle vérifiée
            is_anomaly = source.get('is_anomaly', 'false') == 'true'
            label = source.get('bgl_label', '-')

            entree = {
                "type": "bgl_log",
                "timestamp": source.get('@timestamp', ''),
                "label": label,
                "component": source.get('bgl_component', ''),
                "level": source.get('bgl_level', ''),
                "content": source.get('bgl_content', ''),
                "is_anomaly": is_anomaly,  # ground truth uniquement
                "collecte_a": datetime.utcnow().isoformat()
            }

            # Stocker TOUS les logs sans filtrer
            r.lpush("bgl_logs_all", json.dumps(entree))
            compteur_total += 1

            if is_anomaly:
                compteur_anomalies += 1
            else:
                compteur_normaux += 1

        r.ltrim("bgl_logs_all", 0, 1999)

        print(f"[LOGS] Total: {compteur_total} | "
              f"Normaux: {compteur_normaux} | "
              f"Anomalies ground truth: {compteur_anomalies}")

    except Exception as e:
        print(f"[LOGS] Erreur : {e}")


def collecter_metriques_prometheus():
    try:
        base_url = "http://localhost:9090/api/v1/query"

        def get_val(query):
            try:
                resp = requests.get(base_url, params={"query": query})
                data = resp.json()
                results = data['data']['result']
                if results:
                    return round(sum(float(r['value'][1]) for r in results), 4)
            except:
                pass
            return 0.0

        entree = {
            "type": "metrique",
            "timestamp": datetime.utcnow().isoformat(),
            "debit_logs_par_sec": get_val("bgl_replayer_lines_per_second"),
            "cpu_iterations_par_sec": get_val("rate(bgl_replayer_cpu_iterations_total[1m])"),
            "ram_pct": get_val("100*(1-(node_memory_MemAvailable_bytes/node_memory_MemTotal_bytes))"),
            "cpu_pct": get_val('100-(avg(rate(node_cpu_seconds_total{mode="idle"}[1m]))*100)'),
            "anomalies_injectees_5min": get_val("increase(bgl_replayer_anomalies_injected_total[5m])"),
            "collecte_a": datetime.utcnow().isoformat()
        }

        r.lpush("bgl_metriques", json.dumps(entree))
        r.ltrim("bgl_metriques", 0, 999)

        print(f"[METRIQUES] CPU: {entree['cpu_pct']:.1f}% | "
              f"RAM: {entree['ram_pct']:.1f}% | "
              f"Débit: {entree['debit_logs_par_sec']:.2f} logs/sec | "
              f"Anomalies 5min: {entree['anomalies_injectees_5min']:.0f}")

    except Exception as e:
        print(f"[METRIQUES] Erreur : {e}")


def afficher_stats_redis():
    nb_logs = r.llen("bgl_logs_all")
    nb_metriques = r.llen("bgl_metriques")
    print(f"[REDIS] Logs total: {nb_logs} | Métriques: {nb_metriques}")


# Nettoyer les anciennes clés
r.delete("bgl_logs_anomalies")

print("Collecte toutes les 30 secondes... Ctrl+C pour arrêter")
print("-" * 50)

while True:
    collecter_logs_bgl()
    collecter_metriques_prometheus()
    afficher_stats_redis()
    print("-" * 50)
    time.sleep(60)
