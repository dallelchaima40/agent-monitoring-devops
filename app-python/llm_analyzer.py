import redis
import json
from google import genai
from datetime import datetime

r = redis.Redis(host='redis', port=6379, decode_responses=True)


# client = genai.Client(api_key="")

def charger_contexte_redis():
    # Charger les logs ERROR bruts
    logs_errors = []
    for i in range(min(10, r.llen("logs_errors"))):
        log_json = r.lindex("logs_errors", i)
        if log_json:
            log = json.loads(log_json)
            logs_errors.append(log.get("message", ""))

    # Charger les templates Drain
    templates_count = {}
    for i in range(min(50, r.llen("logs_parsed"))):
        log_json = r.lindex("logs_parsed", i)
        if log_json:
            log = json.loads(log_json)
            template = log.get("template", "")
            if template:
                templates_count[template] = templates_count.get(template, 0) + 1

    return logs_errors, templates_count


def construire_prompt(logs_errors, templates_count):
    prompt = """Tu es un expert en DevOps et en sécurité informatique.
Analyse les données de monitoring suivantes et génère un rapport détaillé en français.

## DONNÉES DE MONITORING

### 1. Derniers logs ERROR collectés
"""
    for log in logs_errors:
        prompt += f"- {log}\n"

    prompt += """
### 2. Templates de logs identifiés par Drain
"""
    for template, count in sorted(templates_count.items(),
                                   key=lambda x: x[1], reverse=True):
        prompt += f"- [{count}x] {template}\n"

    prompt += """
## MISSION

Génère un rapport structuré avec :

1. **Résumé exécutif** (2-3 phrases sur l'état général du système)
2. **Anomalies critiques** (liste des problèmes les plus graves avec leur impact)
3. **Analyse des causes** (explique pourquoi ces anomalies se produisent)
4. **Patterns identifiés** (quels types d'anomalies reviennent le plus souvent)
5. **Recommandations** (3-5 actions concrètes pour résoudre les problèmes)
6. **Niveau d'alerte** (CRITIQUE / ÉLEVÉ / MODÉRÉ / FAIBLE) avec justification

Sois précis, concis et actionnable.
"""
    return prompt


def analyser_avec_gemini(prompt):
    print("Envoi des données à Gemini pour analyse...")

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt
    )

    return response.text


def stocker_rapport_redis(rapport):
    entree = {
        "timestamp": datetime.utcnow().isoformat(),
        "rapport": rapport
    }
    r.set("rapport_llm", json.dumps(entree))
    r.lpush("historique_rapports", json.dumps(entree))
    r.ltrim("historique_rapports", 0, 9)
    print("Rapport stocké dans Redis sous la clé 'rapport_llm'")


# Programme principal
print("Démarrage de l'analyseur LLM Gemini...")
print("-" * 50)

logs_errors, templates_count = charger_contexte_redis()

print(f"Logs ERROR chargés    : {len(logs_errors)}")
print(f"Templates Drain chargés : {len(templates_count)}")
print("-" * 50)


print("\nLogs ERROR chargés :")
print("-" * 50)
for log in logs_errors:
    print(f"  - {log}")

print("\nTemplates Drain chargés :")
print("-" * 50)
for template, count in sorted(templates_count.items(),
                               key=lambda x: x[1], reverse=True):
    print(f"  [{count:3d}x] {template}")
print("-" * 50)


prompt = construire_prompt(logs_errors, templates_count)
rapport = analyser_avec_gemini(prompt)

print(f"\n{'='*60}")
print("RAPPORT D'ANALYSE LLM GEMINI")
print('='*60)
print(rapport)
print('='*60)

stocker_rapport_redis(rapport)
print("\nAnalyse LLM terminée avec succès !")