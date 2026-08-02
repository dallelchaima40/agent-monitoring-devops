"""
Agent DevOps IA — LangGraph
Pipeline automatique :
infra_check → collect_data → run_ml_pipeline → run_llm_analyzer
→ decide_severite → send_alert (si CRITIQUE) ou log_only (si normal)
"""

import os
import json
import time
import subprocess
import redis
import requests
from datetime import datetime
from typing import TypedDict, Literal
from dotenv import load_dotenv

from langgraph.graph import StateGraph, END
from langchain_core.runnables import RunnableLambda

load_dotenv("/workspaces/agent-monitoring-devops/.env")

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# -------------------------------------------------------
# État partagé entre tous les noeuds
# -------------------------------------------------------
class AgentState(TypedDict):
    timestamp: str
    infra_ok: bool
    infra_details: dict
    collect_ok: bool
    ml_ok: bool
    ml_results: dict
    llm_ok: bool
    rapport: str
    severite: str
    alerte_envoyee: bool
    erreurs: list
    logs_execution: list


# -------------------------------------------------------
# NOEUD 1 — infra_check
# -------------------------------------------------------
def infra_check(state: AgentState) -> AgentState:
    print("\n" + "="*50)
    print("[AGENT] NOEUD 1 — Vérification infrastructure")
    print("="*50)

    details = {}
    erreurs = []

    # Vérifier Elasticsearch
    try:
        resp = requests.get("http://localhost:9200", timeout=5)
        if resp.status_code == 200:
            details["elasticsearch"] = "OK"
            print("  ✓ Elasticsearch OK")
        else:
            details["elasticsearch"] = f"ERREUR {resp.status_code}"
            erreurs.append("Elasticsearch ne répond pas correctement")
    except Exception as e:
        details["elasticsearch"] = f"ERREUR: {e}"
        erreurs.append(f"Elasticsearch inaccessible: {e}")
        print(f"  ✗ Elasticsearch ERREUR: {e}")

    # Vérifier Redis
    try:
        r.ping()
        details["redis"] = "OK"
        nb_logs = r.llen("bgl_logs_all")
        details["redis_logs"] = nb_logs
        print(f"  ✓ Redis OK ({nb_logs} logs dans bgl_logs_all)")
    except Exception as e:
        details["redis"] = f"ERREUR: {e}"
        erreurs.append(f"Redis inaccessible: {e}")
        print(f"  ✗ Redis ERREUR: {e}")

    # Vérifier le replayer
    try:
        resp = requests.get("http://localhost:8000/metrics", timeout=5)
        if "bgl_replayer_lines_per_second" in resp.text:
            details["replayer"] = "OK"
            print("  ✓ Replayer OK")
        else:
            details["replayer"] = "ERREUR métriques manquantes"
            erreurs.append("Replayer ne produit pas de métriques")
    except Exception as e:
        details["replayer"] = f"ERREUR: {e}"
        erreurs.append(f"Replayer inaccessible: {e}")
        print(f"  ✗ Replayer ERREUR: {e}")

    # Vérifier Prometheus
    try:
        resp = requests.get("http://localhost:9090/api/v1/query?query=up", timeout=5)
        data = resp.json()
        nb_targets = len(data["data"]["result"])
        details["prometheus"] = f"OK ({nb_targets} cibles actives)"
        print(f"  ✓ Prometheus OK ({nb_targets} cibles)")
    except Exception as e:
        details["prometheus"] = f"ERREUR: {e}"
        erreurs.append(f"Prometheus inaccessible: {e}")
        print(f"  ✗ Prometheus ERREUR: {e}")

    infra_ok = len(erreurs) == 0

    state["infra_ok"] = infra_ok
    state["infra_details"] = details
    state["erreurs"] = erreurs
    state["logs_execution"].append({
        "noeud": "infra_check",
        "timestamp": datetime.utcnow().isoformat(),
        "resultat": "OK" if infra_ok else "ERREUR",
        "details": details
    })

    if infra_ok:
        print("  → Infrastructure opérationnelle")
    else:
        print(f"  → {len(erreurs)} problème(s) détecté(s)")

    return state


# -------------------------------------------------------
# NOEUD 2 — collect_data
# -------------------------------------------------------
def collect_data(state: AgentState) -> AgentState:
    print("\n" + "="*50)
    print("[AGENT] NOEUD 2 — Collecte des données")
    print("="*50)

    try:
        # Vérifier s'il y a déjà des données récentes dans Redis
        nb_logs = r.llen("bgl_logs_all")
        nb_metriques = r.llen("bgl_metriques")

        print(f"  Logs existants dans Redis : {nb_logs}")
        print(f"  Métriques existantes      : {nb_metriques}")

        if nb_logs >= 50 and nb_metriques >= 5:
            print("  → Données suffisantes, pas besoin de recollecte")
            state["collect_ok"] = True
        else:
            print("  → Lancement du collecteur (60 secondes)...")
            result = subprocess.run(
                ["python3",
                 "/workspaces/agent-monitoring-devops/collector_redis.py"],
                timeout=70,
                capture_output=True,
                text=True
            )
            print(result.stdout[-500:] if result.stdout else "")
            state["collect_ok"] = result.returncode == 0

        state["logs_execution"].append({
            "noeud": "collect_data",
            "timestamp": datetime.utcnow().isoformat(),
            "resultat": "OK" if state["collect_ok"] else "ERREUR",
            "nb_logs": r.llen("bgl_logs_all"),
            "nb_metriques": r.llen("bgl_metriques")
        })

    except subprocess.TimeoutExpired:
        print("  → Timeout collecteur — on continue avec les données existantes")
        state["collect_ok"] = r.llen("bgl_logs_all") >= 50
    except Exception as e:
        print(f"  ✗ Erreur collecte : {e}")
        state["collect_ok"] = False
        state["erreurs"].append(f"Erreur collecte: {e}")

    return state


# -------------------------------------------------------
# NOEUD 3 — run_ml_pipeline
# -------------------------------------------------------
def run_ml_pipeline(state: AgentState) -> AgentState:
    print("\n" + "="*50)
    print("[AGENT] NOEUD 3 — Pipeline ML (One-Class SVM + K-Means)")
    print("="*50)

    try:
        result = subprocess.run(
            ["python3",
             "/workspaces/agent-monitoring-devops/ml_pipeline.py"],
            timeout=120,
            capture_output=True,
            text=True
        )

        print(result.stdout[-1000:] if result.stdout else "")

        if result.returncode == 0:
            ml_json = r.get("ml_results")
            if ml_json:
                state["ml_results"] = json.loads(ml_json)
                resume = state["ml_results"].get("resume", {})
                print(f"\n  ✓ ML terminé :")
                print(f"    Anomalies détectées : {resume.get('anomalies_detectees', 0)}")
                print(f"    F1-Score            : {resume.get('f1_score', 0):.3f}")
                state["ml_ok"] = True
            else:
                state["ml_ok"] = False
                state["erreurs"].append("ml_results manquant dans Redis")
        else:
            state["ml_ok"] = False
            state["erreurs"].append(f"ML pipeline erreur: {result.stderr[-200:]}")
            print(f"  ✗ Erreur ML: {result.stderr[-200:]}")

    except subprocess.TimeoutExpired:
        state["ml_ok"] = False
        state["erreurs"].append("ML pipeline timeout")
        print("  ✗ Timeout ML pipeline")
    except Exception as e:
        state["ml_ok"] = False
        state["erreurs"].append(f"Erreur ML: {e}")
        print(f"  ✗ Erreur ML: {e}")

    state["logs_execution"].append({
        "noeud": "run_ml_pipeline",
        "timestamp": datetime.utcnow().isoformat(),
        "resultat": "OK" if state["ml_ok"] else "ERREUR"
    })

    return state


# -------------------------------------------------------
# NOEUD 4 — run_llm_analyzer
# -------------------------------------------------------
def run_llm_analyzer(state: AgentState) -> AgentState:
    print("\n" + "="*50)
    print("[AGENT] NOEUD 4 — Analyse LLM Gemini")
    print("="*50)

    try:
        result = subprocess.run(
            ["python3",
             "/workspaces/agent-monitoring-devops/llm_analyzer_bgl.py"],
            timeout=60,
            capture_output=True,
            text=True
        )

        print(result.stdout[-2000:] if result.stdout else "")

        if result.returncode == 0:
            rapport_json = r.get("rapport_llm_bgl")
            if rapport_json:
                rapport_data = json.loads(rapport_json)
                state["rapport"] = rapport_data.get("rapport", "")
                state["llm_ok"] = True
                print(f"\n  ✓ Rapport LLM généré ({len(state['rapport'])} caractères)")
            else:
                state["llm_ok"] = False
                state["erreurs"].append("rapport_llm_bgl manquant dans Redis")
        else:
            state["llm_ok"] = False
            state["erreurs"].append(f"LLM erreur: {result.stderr[-200:]}")

    except Exception as e:
        state["llm_ok"] = False
        state["erreurs"].append(f"Erreur LLM: {e}")
        print(f"  ✗ Erreur LLM: {e}")

    state["logs_execution"].append({
        "noeud": "run_llm_analyzer",
        "timestamp": datetime.utcnow().isoformat(),
        "resultat": "OK" if state["llm_ok"] else "ERREUR"
    })

    return state


# -------------------------------------------------------
# NOEUD 5 — decide_severite (branchement conditionnel)
# -------------------------------------------------------
def decide_severite(state: AgentState) -> AgentState:
    print("\n" + "="*50)
    print("[AGENT] NOEUD 5 — Décision sévérité")
    print("="*50)

    rapport = state.get("rapport", "")
    ml_results = state.get("ml_results", {})
    resume = ml_results.get("resume", {})

    taux_anomalie = resume.get("taux_anomalie_pct", 0)
    f1_score = resume.get("f1_score", 0)
    clusters = ml_results.get("clusters", [])

    nb_critique = sum(1 for c in clusters if c.get("severite") == "CRITIQUE")
    nb_eleve = sum(1 for c in clusters if c.get("severite") == "ÉLEVÉE")

    # Règles de décision
    if ("CRITIQUE" in rapport.upper() and taux_anomalie > 10) or nb_critique >= 2:
        severite = "CRITIQUE"
    elif ("ÉLEVÉ" in rapport.upper() and taux_anomalie > 5) or nb_critique >= 1 or nb_eleve >= 2:
        severite = "ÉLEVÉ"
    elif taux_anomalie > 2 or nb_eleve >= 1:
        severite = "MODÉRÉ"
    else:
        severite = "FAIBLE"

    state["severite"] = severite

    print(f"  Taux anomalies   : {taux_anomalie:.1f}%")
    print(f"  Clusters CRITIQUE: {nb_critique}")
    print(f"  Clusters ÉLEVÉ   : {nb_eleve}")
    print(f"  → Sévérité finale : {severite}")

    state["logs_execution"].append({
        "noeud": "decide_severite",
        "timestamp": datetime.utcnow().isoformat(),
        "severite": severite,
        "taux_anomalie": taux_anomalie
    })

    return state


# -------------------------------------------------------
# NOEUD 6a — send_alert (si CRITIQUE ou ÉLEVÉ)
# -------------------------------------------------------
def send_alert(state: AgentState) -> AgentState:
    print("\n" + "="*50)
    print(f"[AGENT] NOEUD 6 — ALERTE {state['severite']}")
    print("="*50)

    alerte = {
        "timestamp": datetime.utcnow().isoformat(),
        "severite": state["severite"],
        "taux_anomalie": state["ml_results"].get("resume", {}).get("taux_anomalie_pct", 0),
        "nb_clusters": len(state["ml_results"].get("clusters", [])),
        "extrait_rapport": state["rapport"][:500] if state["rapport"] else "",
        "infra": state["infra_details"]
    }

    r.lpush("alertes", json.dumps(alerte))
    r.ltrim("alertes", 0, 99)
    r.set("derniere_alerte", json.dumps(alerte))

    print(f"  🚨 ALERTE {state['severite']} stockée dans Redis")
    print(f"  Taux anomalies : {alerte['taux_anomalie']:.1f}%")
    print(f"  Clusters       : {alerte['nb_clusters']}")
    print(f"\n  Extrait rapport :")
    print(f"  {alerte['extrait_rapport'][:300]}")

    state["alerte_envoyee"] = True
    state["logs_execution"].append({
        "noeud": "send_alert",
        "timestamp": datetime.utcnow().isoformat(),
        "severite": state["severite"]
    })

    return state


# -------------------------------------------------------
# NOEUD 6b — log_only (si MODÉRÉ ou FAIBLE)
# -------------------------------------------------------
def log_only(state: AgentState) -> AgentState:
    print("\n" + "="*50)
    print(f"[AGENT] NOEUD 6 — Système normal ({state['severite']})")
    print("="*50)

    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "severite": state["severite"],
        "taux_anomalie": state["ml_results"].get("resume", {}).get("taux_anomalie_pct", 0),
        "message": "Système dans les paramètres normaux"
    }

    r.lpush("logs_monitoring", json.dumps(log_entry))
    r.ltrim("logs_monitoring", 0, 999)

    print(f"  ✓ Système {state['severite']} — log enregistré")
    print(f"  Taux anomalies : {log_entry['taux_anomalie']:.1f}%")

    state["alerte_envoyee"] = False
    state["logs_execution"].append({
        "noeud": "log_only",
        "timestamp": datetime.utcnow().isoformat(),
        "severite": state["severite"]
    })

    return state


# -------------------------------------------------------
# Routeur conditionnel
# -------------------------------------------------------
def router_infra(state: AgentState) -> Literal["collect_data", "end_infra_error"]:
    if state["infra_ok"]:
        return "collect_data"
    else:
        return "end_infra_error"
    
def end_infra_error(state: AgentState) -> AgentState:
    state["severite"] = "CRITIQUE"
    state["rapport"] = f"Infrastructure défaillante : {state['erreurs']}"
    print(f"\n  Infrastructure KO — arrêt de l'agent")
    print(f"  Erreurs : {state['erreurs']}")
    return state

def router_collect(state: AgentState) -> Literal["run_ml_pipeline", "send_alert"]:
    if state["collect_ok"]:
        return "run_ml_pipeline"
    else:
        state["severite"] = "ÉLEVÉ"
        return "send_alert"

def router_ml(state: AgentState) -> Literal["run_llm_analyzer", "send_alert"]:
    if state["ml_ok"]:
        return "run_llm_analyzer"
    else:
        state["severite"] = "ÉLEVÉ"
        return "send_alert"

def router_llm(state: AgentState) -> Literal["decide_severite", "decide_severite"]:
    return "decide_severite"

def router_severite(state: AgentState) -> Literal["send_alert", "log_only"]:
    if state["severite"] in ["CRITIQUE", "ÉLEVÉ"]:
        return "send_alert"
    else:
        return "log_only"


# -------------------------------------------------------
# Construction du graphe LangGraph
# -------------------------------------------------------
def build_agent():
    graph = StateGraph(AgentState)

    graph.add_node("infra_check", infra_check)
    graph.add_node("end_infra_error", end_infra_error)
    graph.add_node("collect_data", collect_data)
    graph.add_node("run_ml_pipeline", run_ml_pipeline)
    graph.add_node("run_llm_analyzer", run_llm_analyzer)
    graph.add_node("decide_severite", decide_severite)
    graph.add_node("send_alert", send_alert)
    graph.add_node("log_only", log_only)

    graph.set_entry_point("infra_check")

    graph.add_conditional_edges("infra_check", router_infra)
    graph.add_edge("end_infra_error", END)
    graph.add_conditional_edges("collect_data", router_collect)
    graph.add_conditional_edges("run_ml_pipeline", router_ml)
    graph.add_edge("run_llm_analyzer", "decide_severite")
    graph.add_conditional_edges("decide_severite", router_severite)
    graph.add_edge("send_alert", END)
    graph.add_edge("log_only", END)

    return graph.compile()

# -------------------------------------------------------
# Point d'entrée
# -------------------------------------------------------
if __name__ == "__main__":
    print("\n" + "="*60)
    print("AGENT DEVOPS IA — LangGraph")
    print(f"Démarrage : {datetime.utcnow().isoformat()}")
    print("="*60)

    agent = build_agent()

    etat_initial = AgentState(
        timestamp=datetime.utcnow().isoformat(),
        infra_ok=False,
        infra_details={},
        collect_ok=False,
        ml_ok=False,
        ml_results={},
        llm_ok=False,
        rapport="",
        severite="FAIBLE",
        alerte_envoyee=False,
        erreurs=[],
        logs_execution=[]
    )

    etat_final = agent.invoke(etat_initial)

    print("\n" + "="*60)
    print("RÉSUMÉ FINAL DE L'AGENT")
    print("="*60)
    print(f"  Infrastructure  : {'OK' if etat_final['infra_ok'] else 'ERREUR'}")
    print(f"  Collecte        : {'OK' if etat_final['collect_ok'] else 'ERREUR'}")
    print(f"  ML Pipeline     : {'OK' if etat_final['ml_ok'] else 'ERREUR'}")
    print(f"  LLM Analyzer    : {'OK' if etat_final['llm_ok'] else 'ERREUR'}")
    print(f"  Sévérité finale : {etat_final['severite']}")
    print(f"  Alerte envoyée  : {etat_final['alerte_envoyee']}")
    print(f"  Erreurs         : {len(etat_final['erreurs'])}")

    r.set("agent_last_run", json.dumps({
        "timestamp": datetime.utcnow().isoformat(),
        "severite": etat_final["severite"],
        "alerte": etat_final["alerte_envoyee"],
        "logs": etat_final["logs_execution"]
    }))

    print("\n  État final stocké dans Redis : 'agent_last_run'")
    print("="*60)
