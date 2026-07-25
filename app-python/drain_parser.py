import redis
import json
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from datetime import datetime

r = redis.Redis(host='redis', port=6379, decode_responses=True)

# Configuration Drain
config = TemplateMinerConfig()
config.drain_depth = 4
config.drain_sim_th = 0.5
config.drain_max_children = 100
miner = TemplateMiner(config=config)

print("Démarrage du parser Drain...")
print(f"Logs disponibles dans Redis : {r.llen('logs_errors')}")
print("-" * 50)

def parser_logs():
    nb_logs = r.llen("logs_errors")
    if nb_logs == 0:
        print("Aucun log dans Redis.")
        return {}, []

    templates_trouves = {}
    logs_parses = []

    for i in range(nb_logs):
        log_json = r.lindex("logs_errors", i)
        if not log_json:
            continue

        log = json.loads(log_json)
        message = log.get("message", "")

        if not message:
            continue

        # Extraire seulement la partie message sans le préfixe timestamp
        if ": ERROR " in message:
            message_clean = message.split(": ERROR ")[-1]
        elif ": WARNING " in message:
            message_clean = message.split(": WARNING ")[-1]
        else:
            message_clean = message

        # Parser avec Drain
        result = miner.add_log_message(message_clean)
        template = result["template_mined"]
        cluster_id = result["cluster_id"]

        # Compter les occurrences
        if template not in templates_trouves:
            templates_trouves[template] = 0
        templates_trouves[template] += 1

        # Enrichir le log
        log_enrichi = {
            **log,
            "message_clean": message_clean,
            "template": template,
            "cluster_id": cluster_id,
            "parse_a": datetime.utcnow().isoformat()
        }
        logs_parses.append(log_enrichi)

        # Stocker dans Redis
        r.lpush("logs_parsed", json.dumps(log_enrichi))

    # Garder seulement les 1000 derniers
    r.ltrim("logs_parsed", 0, 999)

    print(f"Templates découverts par Drain :")
    print("-" * 50)
    for template, count in sorted(templates_trouves.items(),
                                   key=lambda x: x[1], reverse=True):
        print(f"  [{count:3d}x] {template}")

    print(f"\nTotal logs parsés    : {len(logs_parses)}")
    print(f"Total templates uniques : {len(templates_trouves)}")

    return templates_trouves, logs_parses

templates, logs = parser_logs()