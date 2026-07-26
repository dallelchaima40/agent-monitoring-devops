FROM python:3.12-slim

# stress-ng : genere de vrais pics de CPU/memoire quand une anomalie du
# dataset est rejouee (mecanisme 2 du replayer, independant du volume).
RUN apt-get update && apt-get install -y --no-install-recommends stress-ng \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY replayer.py .

ENV BGL_LOG_PATH=/data/BGL.log \
    OUTPUT_LOG_PATH=/var/log/replay/bgl_replay.log \
    REPLAY_SPEED=60 \
    MAX_LINES=0 \
    METRICS_PORT=8000 \
    CPU_WORK_PER_LINE=2000 \
    STRESS_DURATION_S=5

EXPOSE 8000

CMD ["python", "replayer.py"]
