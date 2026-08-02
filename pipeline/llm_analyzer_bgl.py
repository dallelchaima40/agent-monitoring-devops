import redis
import json
import os
from google import genai
from datetime import datetime, timezone
from dotenv import load_dotenv

# Charger .env depuis le dossier racine du projet (deux niveaux au-dessus de pipeline/)
_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT_DIR, ".env"))

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Clé API depuis .env
api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    print("ERREUR : GEMINI_API_KEY manquante dans le fichier .env")
    exit(1)

client = genai.Client(api_key=api_key)

def charger_contexte_redis():
    """Charge les résultats ML depuis Redis"""

    # Résultats du pipeline ML (One-Class SVM + K-Means)
    ml_json = r.get("ml_results")
    if not ml_json:
        print("ERREUR : clé 'ml_results' introuvable. Lance d'abord ml_pipeline.py")
        return None
    ml_results = json.loads(ml_json)

    return ml_results


def construire_prompt(ml_results):
    """Construit un prompt structuré — jamais les logs bruts"""

    resume = ml_results.get("resume", {})
    clusters = ml_results.get("clusters", [])
    top_templates = ml_results.get("top_templates_anomalies", {})
    features_deviation = ml_results.get("features_deviation", {})
    metriques = ml_results.get("metriques_systeme", {})

    prompt = """Tu es un expert en DevOps et en fiabilité des systèmes (SRE).
Tu reçois un résumé structuré produit par un pipeline de Machine Learning
qui a analysé des logs système BGL (supercomputer BlueGene/L).
Tu ne reçois JAMAIS les logs bruts — uniquement des statistiques et scores agrégés.
Génère un rapport opérationnel en français, précis et actionnable.

## RÉSULTATS DU PIPELINE ML

### 1. Détection d'anomalies (One-Class SVM)
"""
    prompt += f"- Logs analysés       : {resume.get('total_logs_analyses', 0)}\n"
    prompt += f"- Normaux détectés    : {resume.get('normaux_detectes', 0)}\n"
    prompt += f"- Anomalies détectées : {resume.get('anomalies_detectees', 0)} "
    prompt += f"({resume.get('taux_anomalie_pct', 0):.1f}% du total)\n"
    prompt += f"- Ground truth BGL    : {resume.get('ground_truth_anomalies', 0)} vraies anomalies\n"
    prompt += f"- Précision modèle    : {resume.get('precision', 0):.3f}\n"
    prompt += f"- Rappel modèle       : {resume.get('rappel', 0):.3f}\n"
    prompt += f"- F1-Score            : {resume.get('f1_score', 0):.3f}\n"

    prompt += "\n### 2. Clustering K-Means (familles d'incidents)\n"
    for c in sorted(clusters, key=lambda x: x.get('score_svm_moyen', 0)):
        prompt += (
            f"- Cluster {c['cluster_id']} | "
            f"Famille: {c['famille_bgl_dominante']} | "
            f"Composant: {c['composant_dominant']} | "
            f"{c['nb_logs']} logs | "
            f"Score SVM: {c['score_svm_moyen']:+.3f} | "
            f"Sévérité: {c['severite']}\n"
            f"  Template dominant: {c['top_template'][:80]}\n"
        )

    prompt += "\n### 3. Templates les plus fréquents dans les anomalies (Drain)\n"
    for template, count in sorted(
        top_templates.items(), key=lambda x: x[1], reverse=True
    ):
        prompt += f"- [{count}x] {template[:100]}\n"

    prompt += "\n### 4. Features les plus déviantes (anomalies vs normaux)\n"
    for feat, deviation in sorted(
        features_deviation.items(), key=lambda x: abs(x[1]), reverse=True
    ):
        direction = "↑ anormal" if deviation > 0 else "↓ anormal"
        prompt += f"- {feat:20s} : déviation {deviation:+.4f} {direction}\n"

    prompt += "\n### 5. Métriques système au moment de l'analyse\n"
    if metriques:
        prompt += f"- CPU usage      : {metriques.get('cpu_pct', 0):.1f}%\n"
        prompt += f"- RAM usage      : {metriques.get('ram_pct', 0):.1f}%\n"
        prompt += f"- Débit logs     : {metriques.get('debit_logs_par_sec', 0):.2f} logs/sec\n"
        prompt += f"- Anomalies 5min : {metriques.get('anomalies_injectees_5min', 0):.0f}\n"

    prompt += """
## MISSION

Génère un rapport opérationnel structuré avec :

1. **Résumé exécutif** (2-3 phrases sur l'état général du système)

2. **Anomalies critiques** (basé sur les clusters avec score SVM le plus négatif et sévérité CRITIQUE)

3. **Analyse des causes** (interprète les familles BGL détectées : APPREAD = problème lecture app, KERNDTLB = erreur TLB kernel, etc.)

4. **Qualité de la détection** (commente le F1-Score et ce que ça signifie pour la fiabilité du monitoring)

5. **Recommandations** (3-5 actions concrètes basées sur les familles d'incidents détectées)

6. **Niveau d'alerte global** (CRITIQUE / ÉLEVÉ / MODÉRÉ / FAIBLE) avec justification basée sur le taux d'anomalies et la sévérité des clusters

Ne jamais inventer de données. Baser l'analyse uniquement sur les chiffres fournis.
"""
    return prompt


def analyser_avec_gemini(prompt):
    print("Envoi du résumé structuré à Gemini...")
    response = client.models.generate_content(
        model="gemini-flash-latest",
        contents=prompt
    )
    return response.text


def stocker_rapport_redis(rapport, ml_results):
    entree = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "rapport": rapport,
        "resume_ml": ml_results.get("resume", {}),
        "nb_clusters": len(ml_results.get("clusters", []))
    }
    r.set("rapport_llm_bgl", json.dumps(entree))
    r.lpush("historique_rapports_bgl", json.dumps(entree))
    r.ltrim("historique_rapports_bgl", 0, 9)
    print("Rapport stocké dans Redis : 'rapport_llm_bgl'")


def sauvegarder_rapport_md(rapport, ml_results):
    """Sauvegarde le rapport dans un fichier Markdown horodaté"""
    # Dossier rapports/ à la racine du projet (deux niveaux au-dessus de pipeline/)
    dossier = os.path.join(_ROOT_DIR, "rapports")
    os.makedirs(dossier, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    nom_fichier = f"rapport_bgl_{timestamp}.md"
    chemin = os.path.join(dossier, nom_fichier)

    resume = ml_results.get("resume", {})

    entete = f"""# Rapport d'analyse BGL — {timestamp}

**Logs analysés :** {resume.get('total_logs_analyses', 0)}
**Anomalies détectées :** {resume.get('anomalies_detectees', 0)} ({resume.get('taux_anomalie_pct', 0):.1f}%)
**F1-Score :** {resume.get('f1_score', 0):.3f}

---

"""

    with open(chemin, "w", encoding="utf-8") as f:
        f.write(entete)
        f.write(rapport)

    print(f"Rapport Markdown sauvegardé : {chemin}")
    return chemin


# Programme principal
print("=" * 60)
print("ANALYSEUR LLM BGL — Gemini 2.5 Flash")
print("=" * 60)

ml_results = charger_contexte_redis()
if not ml_results:
    exit(1)

resume = ml_results.get("resume", {})
print(f"\nRésumé ML chargé depuis Redis :")
print(f"  Logs analysés    : {resume.get('total_logs_analyses', 0)}")
print(f"  Anomalies SVM    : {resume.get('anomalies_detectees', 0)}")
print(f"  Clusters K-Means : {len(ml_results.get('clusters', []))}")
print(f"  F1-Score         : {resume.get('f1_score', 0):.3f}")
print("-" * 60)

prompt = construire_prompt(ml_results)
rapport = analyser_avec_gemini(prompt)

print(f"\n{'='*60}")
print("RAPPORT D'ANALYSE LLM GEMINI")
print('='*60)
print(rapport)
print('='*60)

stocker_rapport_redis(rapport, ml_results)
sauvegarder_rapport_md(rapport, ml_results)
print("\nAnalyse LLM terminée avec succès !")
