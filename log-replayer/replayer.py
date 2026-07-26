"""
log-replayer : rejoue un dataset de logs BGL (Loghub) en respectant (a un
facteur d'acceleration pres) le rythme reel des evenements.

Ce script fait DEUX choses en meme temps :
  1. Il ecrit chaque ligne rejouee dans un fichier que Filebeat surveille,
     pour alimenter la stack ELK comme si c'etait un vrai serveur qui logue.
  2. Il genere une VRAIE charge CPU/memoire (pas une metrique fabriquee) :
       - Mecanisme "volume" : un calcul CPU (hash repete) dont le nombre
         d'iterations est proportionnel au debit instantane de logs rejoues.
         Plus il y a de lignes/seconde, plus le CPU reel du conteneur monte.
       - Mecanisme "stress ponctuel" : quand une VRAIE anomalie du dataset
         est rejouee (Label != "-"), un pic reel de CPU+memoire est declenche
         via stress-ng, INDEPENDAMMENT du volume. Ca simule les anomalies qui
         n'ont rien a voir avec la charge (fuite memoire, probleme materiel...).

Prometheus/cAdvisor/node-exporter mesurent donc un comportement systeme
authentique, pas une statistique injectee artificiellement.

NOTE (contournement cAdvisor) : sur certains hotes Docker recents utilisant
le backend de stockage "containerd snapshotter" (au lieu du graphdriver
overlay2 classique), cAdvisor echoue a identifier les conteneurs
individuels ("failed to identify the read-write layer ID"). Pour ne pas
dependre de cAdvisor pour SES PROPRES metriques, ce script lit aussi
directement ses fichiers cgroup v2 (memory.current, cpu.stat) et les
expose lui-meme en Prometheus - une mesure fiable de sa propre
consommation, independante de cAdvisor.

Variables d'environnement :
    BGL_LOG_PATH        chemin du fichier de logs BGL brut (format Loghub)
    OUTPUT_LOG_PATH      fichier de sortie surveille par Filebeat
    REPLAY_SPEED         facteur d'acceleration (60 = 1s reelle = 60s dataset)
    MAX_LINES            0 = pas de limite, sinon arrete apres N lignes
    METRICS_PORT          port d'exposition des metriques Prometheus
    CPU_WORK_PER_LINE     iterations de hash "de base" par ligne rejouee
    STRESS_DURATION_S      duree (secondes) de chaque pic de stress reel
"""

import hashlib
import logging
import os
import re
import shutil
import subprocess
import threading
import time
from collections import deque

from prometheus_client import Counter, Gauge, start_http_server

# --- Configuration ------------------------------------------------------
BGL_LOG_PATH = os.environ.get("BGL_LOG_PATH", "/data/BGL.log")
OUTPUT_LOG_PATH = os.environ.get("OUTPUT_LOG_PATH", "/var/log/replay/bgl_replay.log")
REPLAY_SPEED = float(os.environ.get("REPLAY_SPEED", "60"))
MAX_LINES = int(os.environ.get("MAX_LINES", "0"))
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8000"))
CPU_WORK_PER_LINE = int(os.environ.get("CPU_WORK_PER_LINE", "2000"))
STRESS_DURATION_S = int(os.environ.get("STRESS_DURATION_S", "5"))

STRESS_NG_AVAILABLE = shutil.which("stress-ng") is not None

# --- Contournement cAdvisor : lecture directe des fichiers cgroup v2 -----
# (utile quand Docker utilise le backend "containerd snapshotter", que
# cAdvisor ne sait pas encore lire correctement pour identifier les
# conteneurs individuels - voir note en tete de fichier).
CGROUP_MEMORY_PATH = "/sys/fs/cgroup/memory.current"
CGROUP_CPU_STAT_PATH = "/sys/fs/cgroup/cpu.stat"
CGROUP_REPORT_INTERVAL_S = 2.0

# --- Format brut BGL (Loghub) --------------------------------------------
# Label Timestamp Date Node Time NodeRepeat Type Component Level Content
BGL_LINE_RE = re.compile(
    r"^(?P<label>\S+)\s+(?P<timestamp>\d+)\s+(?P<date>\S+)\s+(?P<node>\S+)\s+"
    r"(?P<time>\S+)\s+(?P<noderepeat>\S+)\s+(?P<type>\S+)\s+(?P<component>\S+)\s+"
    r"(?P<level>\S+)\s+(?P<content>.*)$"
)

# --- Metriques Prometheus exposees par le replayer lui-meme --------------
lines_total = Counter(
    "bgl_replayer_lines_total", "Nombre de lignes rejouees", ["label_type"]
)
current_rate = Gauge(
    "bgl_replayer_lines_per_second", "Debit instantane de lignes rejouees (fenetre glissante)"
)
cpu_iterations_total = Counter(
    "bgl_replayer_cpu_iterations_total", "Iterations de calcul CPU effectuees (mecanisme volume)"
)
anomalies_injected_total = Counter(
    "bgl_replayer_anomalies_injected_total",
    "Nombre de pics de stress reels declenches (mecanisme anomalie)",
    ["anomaly_label"],
)
# Metriques auto-mesurees via cgroup v2 (contournement cAdvisor, voir note
# en tete de fichier). memory_bytes est une vraie valeur instantanee ;
# cpu_usec_total est un compteur cumulatif du noyau - utiliser rate()/
# increase() dessus en PromQL fonctionne normalement malgre le type Gauge
# cote client (le calcul de rate() ne depend que des valeurs, pas du type
# declare).
container_memory_bytes = Gauge(
    "bgl_replayer_container_memory_bytes",
    "Memoire reelle utilisee par ce conteneur (lue directement depuis cgroup v2)",
)
container_cpu_usec_total = Gauge(
    "bgl_replayer_container_cpu_usec_total",
    "Temps CPU cumule (microsecondes) consomme par ce conteneur (lu directement depuis cgroup v2)",
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [replayer] %(message)s")
logger = logging.getLogger("bgl-replayer")


def read_cgroup_metrics_once():
    """Lit les fichiers cgroup v2 de ce conteneur et met a jour les Gauges
    Prometheus correspondantes. Echoue silencieusement (log en DEBUG
    seulement) si les fichiers ne sont pas accessibles - par exemple si le
    conteneur tourne sur un hote en cgroup v1, ou hors Linux/Docker."""
    try:
        with open(CGROUP_MEMORY_PATH) as f:
            container_memory_bytes.set(int(f.read().strip()))
    except (OSError, ValueError) as e:
        logger.debug("Lecture memoire cgroup impossible : %s", e)

    try:
        with open(CGROUP_CPU_STAT_PATH) as f:
            for line in f:
                if line.startswith("usage_usec"):
                    container_cpu_usec_total.set(int(line.split()[1]))
                    break
    except (OSError, ValueError) as e:
        logger.debug("Lecture CPU cgroup impossible : %s", e)


def start_cgroup_reporter(interval_s=CGROUP_REPORT_INTERVAL_S):
    """Lance un thread daemon qui met a jour les metriques cgroup en continu,
    independamment du rythme de replay (utile meme si le replay est en
    pause ou tres lent)."""
    def _loop():
        while True:
            read_cgroup_metrics_once()
            time.sleep(interval_s)

    threading.Thread(target=_loop, daemon=True).start()


def parse_bgl_line(raw_line):
    """Parse une ligne brute BGL. Retourne None si la ligne ne matche pas
    (ligne vide, ligne malformee, etc.)."""
    m = BGL_LINE_RE.match(raw_line.strip())
    if not m:
        return None
    d = m.groupdict()
    try:
        d["timestamp"] = int(d["timestamp"])
    except ValueError:
        return None
    d["is_anomaly"] = d["label"] != "-"
    return d


def burn_cpu(n_iterations):
    """Travail CPU reel et mesurable (pas un time.sleep) : hash chaine."""
    h = hashlib.sha256(b"bgl-replay")
    for _ in range(n_iterations):
        h.update(h.digest())
    return h.hexdigest()


def inject_stress(anomaly_label, duration_s):
    """Declenche un vrai pic de charge (CPU + memoire) via stress-ng, dans un
    thread separe pour ne pas bloquer le replay pendant que ca tourne.
    Independant du debit de logs : simule une anomalie de ressource qui
    n'a pas de lien direct avec le volume de trafic."""
    anomalies_injected_total.labels(anomaly_label=anomaly_label).inc()
    if not STRESS_NG_AVAILABLE:
        logger.warning(
            "stress-ng indisponible : anomalie '%s' loguee mais aucun stress reel genere "
            "(installe stress-ng dans l'image pour activer ce mecanisme).",
            anomaly_label,
        )
        return

    def _run():
        try:
            subprocess.run(
                [
                    "stress-ng",
                    "--cpu", "2",
                    "--vm", "1", "--vm-bytes", "256M",
                    "--timeout", f"{duration_s}s",
                ],
                check=False,
                capture_output=True,
            )
        except Exception as e:  # pragma: no cover - defensif seulement
            logger.error("Echec du lancement de stress-ng : %s", e)

    threading.Thread(target=_run, daemon=True).start()


def replay(bgl_log_path, output_log_path, replay_speed, max_lines, cpu_work_per_line):
    os.makedirs(os.path.dirname(output_log_path), exist_ok=True)
    prev_timestamp = None
    window = deque(maxlen=50)  # horodatages recents, pour estimer le debit
    n_lines = 0

    with open(bgl_log_path, "r", errors="replace") as src, open(output_log_path, "a") as dst:
        for raw_line in src:
            parsed = parse_bgl_line(raw_line)
            if parsed is None:
                continue

            # --- Respecter le rythme reel (accelere par REPLAY_SPEED) ----
            if prev_timestamp is not None:
                real_delay = max(0.0, (parsed["timestamp"] - prev_timestamp) / replay_speed)
                # Borne haute : evite des pauses de plusieurs heures reelles
                # si le dataset contient de tres grands ecarts temporels.
                time.sleep(min(real_delay, 2.0))
            prev_timestamp = parsed["timestamp"]

            # --- Ecrire la ligne rejouee (Filebeat la surveille) ----------
            dst.write(raw_line if raw_line.endswith("\n") else raw_line + "\n")
            dst.flush()

            # --- Debit instantane (fenetre glissante de 50 lignes) --------
            now = time.time()
            window.append(now)
            rate = len(window) / max(window[-1] - window[0], 1e-6) if len(window) >= 2 else 1.0
            current_rate.set(rate)

            # --- Mecanisme 1 : charge CPU proportionnelle au volume -------
            n_iter = int(cpu_work_per_line * max(rate, 1.0))
            burn_cpu(n_iter)
            cpu_iterations_total.inc(n_iter)

            label_type = "anomaly" if parsed["is_anomaly"] else "normal"
            lines_total.labels(label_type=label_type).inc()

            # --- Mecanisme 2 : stress reel ponctuel si vraie anomalie -----
            if parsed["is_anomaly"]:
                inject_stress(parsed["label"], STRESS_DURATION_S)
                logger.info(
                    "Anomalie rejouee : label=%s component=%s content=%.80s",
                    parsed["label"], parsed["component"], parsed["content"],
                )

            n_lines += 1
            if max_lines and n_lines >= max_lines:
                logger.info("Limite de %d lignes atteinte, arret.", max_lines)
                break

    logger.info("Replay termine : %d lignes rejouees.", n_lines)


if __name__ == "__main__":
    logger.info("Demarrage du serveur de metriques Prometheus sur le port %d", METRICS_PORT)
    start_http_server(METRICS_PORT)
    logger.info("Demarrage du reporter cgroup v2 (contournement cAdvisor, intervalle=%.1fs)",
                CGROUP_REPORT_INTERVAL_S)
    start_cgroup_reporter()
    logger.info(
        "Replay de %s vers %s (vitesse x%.1f, stress-ng %s)",
        BGL_LOG_PATH, OUTPUT_LOG_PATH, REPLAY_SPEED,
        "disponible" if STRESS_NG_AVAILABLE else "INDISPONIBLE",
    )
    replay(BGL_LOG_PATH, OUTPUT_LOG_PATH, REPLAY_SPEED, MAX_LINES, CPU_WORK_PER_LINE)