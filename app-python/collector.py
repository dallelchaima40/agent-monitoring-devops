import redis
import json
import time
import requests
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta

# Connexions
es = Elasticsearch("http://elasticsearch:9200")
r = redis.Redis(host='redis', port=6379, decode_responses=True)

print("Démarrage du collecteur Redis...")
print(f"Connexion Elasticsearch : {'OK' if es.ping() else 'ERREUR'}")
print(f"Connexion Redis : {'OK' if r.ping() else 'ERREUR'}")

def collecter_logs_elasticsearch():
    try:
        # On récupère les logs ERROR des 2 dernières minutes
        maintenant = datetime.utcnow()
        il_y_a_2_min = maintenant - timedelta(minutes=2)

        result = es.search(index="logs-*", body={
            "size": 100,
            "sort": [{"@timestamp": {"order": "desc"}}],
            "query": {
                "bool": {
                    "must": [
                        {"match": {"log_level": "ERROR"}},
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
                "type": "log",
                "timestamp": source.get('@timestamp', ''),
                "niveau": source.get('log_level', 'UNKNOWN'),
                "service": source.get('service_name', 'unknown'),
                "message": source.get('message', ''),
                "collecte_a": datetime.utcnow().isoformat()
            }
            # On stocke dans Redis avec la clé "logs_errors"
            r.lpush("logs_errors", json.dumps(entree))
            compteur += 1

        # On garde seulement les 1000 derniers pour ne pas saturer Redis
        r.ltrim("logs_errors", 0, 999)

        if compteur > 0:
            print(f"[LOGS] {compteur} erreurs stockées dans Redis")

    except Exception as e:
        print(f"[LOGS] Erreur : {e}")


def collecter_metriques_prometheus():
    try:
        base_url = "http://prometheus:9090/api/v1/query"

        # CPU
        cpu_resp = requests.get(base_url, params={
            "query": '100 - (avg(rate(node_cpu_seconds_total{mode="idle"}[1m])) * 100)'
        })
        cpu_data = cpu_resp.json()

        # RAM
        ram_resp = requests.get(base_url, params={
            "query": "100 * (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))"
        })
        ram_data = ram_resp.json()

        # Réseau reçu
        reseau_resp = requests.get(base_url, params={
            "query": 'rate(node_network_receive_bytes_total[1m])'
        })
        reseau_data = reseau_resp.json()

        cpu_val = 0
        ram_val = 0
        reseau_val = 0

        if cpu_data['data']['result']:
            cpu_val = float(cpu_data['data']['result'][0]['value'][1])

        if ram_data['data']['result']:
            ram_val = float(ram_data['data']['result'][0]['value'][1])

        if reseau_data['data']['result']:
            reseau_val = float(reseau_data['data']['result'][0]['value'][1])

        entree = {
            "type": "metrique",
            "timestamp": datetime.utcnow().isoformat(),
            "cpu_pct": round(cpu_val, 2),
            "ram_pct": round(ram_val, 2),
            "reseau_bytes_sec": round(reseau_val, 2),
            "collecte_a": datetime.utcnow().isoformat()
        }

        # On stocke dans Redis avec la clé "metriques"
        r.lpush("metriques", json.dumps(entree))

        # On garde seulement les 1000 dernières métriques
        r.ltrim("metriques", 0, 999)

        print(f"[METRIQUES] CPU: {cpu_val:.1f}% | RAM: {ram_val:.1f}% | Réseau: {reseau_val:.0f} bytes/sec")

    except Exception as e:
        print(f"[METRIQUES] Erreur : {e}")


def afficher_stats_redis():
    nb_logs = r.llen("logs_errors")
    nb_metriques = r.llen("metriques")
    print(f"[REDIS] File d'attente — Logs ERROR: {nb_logs} | Métriques: {nb_metriques}")


# Boucle principale
print("Collecte en cours toutes les 30 secondes...")
print("Ctrl+C pour arrêter")
print("-" * 50)

while True:
    collecter_logs_elasticsearch()
    collecter_metriques_prometheus()
    afficher_stats_redis()
    print("-" * 50)
    time.sleep(30)
