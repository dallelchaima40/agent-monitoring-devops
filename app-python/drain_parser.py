import redis
import json
from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig
from datetime import datetime

# Connexion Redis
r = redis.Redis(host='redis', port=6379, decode_responses=True)

# Configuration Drain
config = TemplateMinerConfig()
config.drain_depth = 4
config.drain_sim_th = 0.5
config.drain_max_children = 100
miner = TemplateMiner(config=config)

print("Démarrage du parser Drain...")
print("-" * 50)

def parser_logs():
    # Lire tous les logs depuis Redis
    nb_logs = r.llen("logs_errors")
    print(f"Nombre de logs à parser : {nb_logs}")
    
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
        
        # Parser le message avec Drain
        result = miner.add_log_message(message)
        template = result["template_mined"]
        cluster_id = result["cluster_id"]
        
        # Compter les occurrences de chaque template
        if template not in templates_trouves:
            templates_trouves[template] = 0
        templates_trouves[template] += 1
        
        # Enrichir le log avec le template
        log_enrichi = {
            **log,
            "template": template,
            "cluster_id": cluster_id,
            "parse_a": datetime.utcnow().isoformat()
        }
        logs_parses.append(log_enrichi)
        
        # Stocker le log enrichi dans Redis
        r.lpush("logs_parsed", json.dumps(log_enrichi))
    
    # Garder seulement les 1000 derniers
    r.ltrim("logs_parsed", 0, 999)
    
    print(f"\nTemplates découverts par Drain :")
    print("-" * 50)
    for template, count in sorted(templates_trouves.items(), 
                                   key=lambda x: x[1], reverse=True):
        print(f"  [{count}x] {template}")
    
    print(f"\nTotal logs parsés : {len(logs_parses)}")
    print(f"Total templates uniques : {len(templates_trouves)}")
    
    return logs_parses, templates_trouves

logs, templates = parser_logs()