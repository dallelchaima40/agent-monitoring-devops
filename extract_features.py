"""
extract_features.py
====================
Construit le dataset final d'entrainement ML en joignant :
  - les LOGS agreges (Elasticsearch, index bgl-replay-*)
  - les METRIQUES (Prometheus : cAdvisor sur le conteneur log-replayer +
    metriques metier exposees par le replayer lui-meme)
sur des fenetres de 1 minute EN TEMPS REEL (wall-clock).

POURQUOI "TEMPS REEL" ET PAS LE TIMESTAMP HISTORIQUE DU DATASET BGL :
----------------------------------------------------------------------
Le champ Elasticsearch `@timestamp` correspond au moment ou Logstash a recu
l'evenement, donc au temps reel d'execution du replay (Filebeat expedie les
lignes quasi immediatement apres qu'elles sont ecrites par le replayer).
Le champ `bgl_timestamp` (et `@bgl_event_time`, ajoute par le filtre `date`
de Logstash) est la date HISTORIQUE d'origine du log dans le dataset BGL
(ex: juin 2005) - utile pour l'analyse de contenu, mais inutilisable pour
aligner les fenetres avec Prometheus, qui mesure lui aussi en temps reel.
On agrege donc toujours sur `@timestamp`, jamais sur `bgl_timestamp`.

Usage :
    # IMPORTANT : le client Python "elasticsearch" doit etre en version 8.x
    # pour parler a un serveur Elasticsearch 8.13.4 (celui du
    # docker-compose.yml). La derniere version du client (9.x, installee par
    # defaut par un simple "pip install elasticsearch") envoie un header de
    # compatibilite "v9" que le serveur 8.13.4 refuse
    # (BadRequestError: media_type_header_exception).
    pip install "elasticsearch==8.13.0" requests pandas --break-system-packages

    python3 extract_features.py \
        --es-host http://localhost:9200 \
        --es-index "bgl-replay-*" \
        --prom-url http://localhost:9090 \
        --start "2026-07-24T10:00:00" \
        --end   "2026-07-24T12:00:00" \
        --output dataset_bgl_replay.csv

Si --start/--end sont omis, le script utilise les 2 dernieres heures par
rapport a maintenant (pratique pour un premier test juste apres un replay).
"""

import argparse
import datetime as dt

import pandas as pd
import requests

try:
    from elasticsearch import Elasticsearch
except ImportError:  # pragma: no cover
    Elasticsearch = None


# ---------------------------------------------------------------------------
# Requetes Prometheus (PromQL) - une par metrique voulue dans le dataset final
# ---------------------------------------------------------------------------
PROM_QUERIES = {
    # Taux d'utilisation CPU reel du conteneur log-replayer, MESURE PAR LE
    # REPLAYER LUI-MEME (cgroup v2 direct) - PAS via cAdvisor, qui echoue a
    # identifier les conteneurs individuels sur les hotes Docker recents
    # utilisant le backend "containerd snapshotter" (cf. diagnostic prealable :
    # erreurs "failed to identify the read-write layer ID" dans les logs
    # cAdvisor). / 1e6 convertit les microsecondes CPU en secondes CPU, pour
    # un resultat directement comparable a un ancien "rate(...seconds_total)".
    "cpu_usage_rate": "rate(bgl_replayer_container_cpu_usec_total[1m]) / 1e6",
    # Memoire reellement utilisee, idem : lue directement depuis cgroup v2.
    "memory_usage_bytes": "bgl_replayer_container_memory_bytes",
    # Debit de logs mesure directement par le replayer (metrique custom)
    "replayer_lines_per_second": "bgl_replayer_lines_per_second",
    # Taux d'iterations CPU "mecanisme volume" (custom)
    "replayer_cpu_iter_rate": "rate(bgl_replayer_cpu_iterations_total[1m])",
    # Taux d'injections de stress reel "mecanisme anomalie" (custom).
    # sum(...) est INDISPENSABLE ici : cette metrique a un label
    # `anomaly_label` (une serie distincte par type d'anomalie BGL), donc
    # sans agregation Prometheus renvoie plusieurs series et le parsing ne
    # recuperait que la premiere (bug corrige - avant, la plupart des
    # injections etaient silencieusement ignorees).
    "replayer_anomalies_rate": "sum(rate(bgl_replayer_anomalies_injected_total[1m]))",
}


def floor_to_minute(t):
    """Arrondit un datetime a la minute inferieure (pour aligner les fenetres
    Elasticsearch et Prometheus sur les memes bornes)."""
    return t.replace(second=0, microsecond=0)


# ---------------------------------------------------------------------------
# 1. Logs (Elasticsearch) - agregation par fenetre de 1 minute
# ---------------------------------------------------------------------------
def fetch_log_features(es, index, start, end, window="1m"):
    """Agrege les logs par fenetre de temps : nombre de lignes, nombre de
    composants distincts touches, nombre de vraies anomalies BGL, et le
    label d'anomalie le plus frequent dans la fenetre (si il y en a).

    Retourne un DataFrame avec une ligne par fenetre, meme si elle ne
    contient aucun log (min_doc_count=0 + extended_bounds).
    """
    query = {
        "size": 0,
        "query": {
            "range": {
                "@timestamp": {"gte": start.isoformat(), "lte": end.isoformat()}
            }
        },
        "aggs": {
            "windows": {
                "date_histogram": {
                    "field": "@timestamp",
                    "fixed_interval": window,
                    "min_doc_count": 0,
                    "extended_bounds": {
                        "min": start.isoformat(),
                        "max": end.isoformat(),
                    },
                },
                "aggs": {
                    "n_components": {
                        "cardinality": {"field": "bgl_component.keyword"}
                    },
                    "n_anomalies": {"filter": {"term": {"is_anomaly": "true"}}},
                    "top_anomaly_label": {
                        "terms": {"field": "anomaly_label.keyword", "size": 1}
                    },
                },
            }
        },
    }

    resp = es.search(index=index, body=query)
    return parse_log_aggregation(resp)


def parse_log_aggregation(resp):
    """Extrait les buckets de la reponse Elasticsearch en DataFrame.
    Separee de fetch_log_features() pour pouvoir etre testee sans serveur ES
    (en lui passant directement une reponse simulee)."""
    buckets = resp["aggregations"]["windows"]["buckets"]
    rows = []
    for b in buckets:
        top_label = None
        label_buckets = b.get("top_anomaly_label", {}).get("buckets", [])
        if label_buckets:
            top_label = label_buckets[0]["key"]
        rows.append(
            {
                "window_start": b["key_as_string"],
                "log_count": b["doc_count"],
                "n_components": b["n_components"]["value"],
                "n_anomalies": b["n_anomalies"]["doc_count"],
                "anomaly_label": top_label,
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
    return df


# ---------------------------------------------------------------------------
# 2. Metriques (Prometheus) - une requete par metrique, meme grille de temps
# ---------------------------------------------------------------------------
def fetch_prometheus_range(prom_url, query, start, end, step="60s"):
    """Interroge /api/v1/query_range et retourne un DataFrame (window_start, value)
    pour une requete PromQL donnee (une seule serie attendue)."""
    params = {
        "query": query,
        "start": start.timestamp(),
        "end": end.timestamp(),
        "step": step,
    }
    resp = requests.get(f"{prom_url}/api/v1/query_range", params=params, timeout=30)
    resp.raise_for_status()
    return parse_prometheus_response(resp.json())


def parse_prometheus_response(payload):
    """Extrait et AGREGE (somme) toutes les series d'une reponse Prometheus
    en un seul DataFrame (window_start, value).

    IMPORTANT : si la requete PromQL n'agrege pas explicitement (pas de
    sum(...)/avg(...)), Prometheus peut renvoyer PLUSIEURS series (une par
    combinaison de labels, ex: une par anomaly_label). Cette fonction les
    somme TOUTES plutot que de ne garder que la premiere - un bug reel de ce
    genre (silencieusement ignorer les autres series) a deja ete rencontre
    sur bgl_replayer_anomalies_injected_total (label anomaly_label).

    Separee de fetch_prometheus_range() pour etre testable sans serveur.
    """
    result = payload.get("data", {}).get("result", [])
    if not result:
        return pd.DataFrame(columns=["window_start", "value"])

    all_series = []
    for series in result:
        values = series["values"]  # [[timestamp_epoch, "valeur_str"], ...]
        s_df = pd.DataFrame(values, columns=["ts", "value"])
        s_df["window_start"] = pd.to_datetime(s_df["ts"], unit="s", utc=True)
        s_df["value"] = s_df["value"].astype(float)
        all_series.append(s_df[["window_start", "value"]])

    combined = pd.concat(all_series, ignore_index=True)
    return combined.groupby("window_start", as_index=False)["value"].sum()


def fetch_all_metrics(prom_url, start, end, step="60s"):
    """Interroge toutes les metriques de PROM_QUERIES et les fusionne en un
    seul DataFrame (une colonne par metrique, indexee par window_start)."""
    merged = None
    for name, query in PROM_QUERIES.items():
        df = fetch_prometheus_range(prom_url, query, start, end, step)
        df = df.rename(columns={"value": name}).set_index("window_start")
        merged = df if merged is None else merged.join(df, how="outer")
    return merged.reset_index() if merged is not None else pd.DataFrame()


# ---------------------------------------------------------------------------
# 3. Jointure logs + metriques + construction du label final
# ---------------------------------------------------------------------------
def build_dataset(es_host, index, prom_url, start, end, window="1m", step="60s"):
    if Elasticsearch is None:
        raise ImportError(
            "Le package 'elasticsearch' n'est pas installe : "
            "pip install elasticsearch --break-system-packages"
        )

    start = floor_to_minute(start)
    end = floor_to_minute(end) + dt.timedelta(minutes=1)

    es = Elasticsearch(es_host)
    logs_df = fetch_log_features(es, index, start, end, window)
    metrics_df = fetch_all_metrics(prom_url, start, end, step)

    df = pd.merge(logs_df, metrics_df, on="window_start", how="outer").sort_values(
        "window_start"
    )

    # Fenetres sans logs (log_count NaN) : 0 ligne, 0 anomalie, 0 composant.
    # Fenetres sans metriques (rare, ex: replayer pas encore demarre) : 0.
    df[["log_count", "n_components", "n_anomalies"]] = df[
        ["log_count", "n_components", "n_anomalies"]
    ].fillna(0)
    metric_cols = list(PROM_QUERIES.keys())
    df[metric_cols] = df[metric_cols].fillna(0)

    # Label final : anomalie si au moins une vraie anomalie BGL est tombee
    # dans cette fenetre de 1 minute.
    df["label"] = (df["n_anomalies"] > 0).astype(int)

    return df.reset_index(drop=True)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--es-host", default="http://localhost:9200")
    parser.add_argument("--es-index", default="bgl-replay-*")
    parser.add_argument("--prom-url", default="http://localhost:9090")
    parser.add_argument("--start", default=None, help="ISO 8601, ex: 2026-07-24T10:00:00")
    parser.add_argument("--end", default=None, help="ISO 8601, ex: 2026-07-24T12:00:00")
    parser.add_argument("--window", default="1m", help="Fenetre d'agregation des logs (Elasticsearch)")
    parser.add_argument("--step", default="60s", help="Pas d'echantillonnage Prometheus")
    parser.add_argument("--output", default="dataset_bgl_replay.csv")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    end = dt.datetime.fromisoformat(args.end) if args.end else dt.datetime.utcnow()
    start = (
        dt.datetime.fromisoformat(args.start)
        if args.start
        else end - dt.timedelta(hours=2)
    )

    print(f"Extraction de {start.isoformat()} a {end.isoformat()} "
          f"(index='{args.es_index}', fenetre={args.window})")

    df = build_dataset(
        es_host=args.es_host,
        index=args.es_index,
        prom_url=args.prom_url,
        start=start,
        end=end,
        window=args.window,
        step=args.step,
    )

    print(f"{len(df)} fenetres extraites, {df['label'].sum()} fenetres avec anomalie "
          f"({100 * df['label'].mean():.1f}%)")
    df.to_csv(args.output, index=False)
    print(f"Dataset sauvegarde dans {args.output}")