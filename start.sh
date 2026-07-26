#!/bin/bash
set -e

echo "=========================================="
echo "  BGL Replay Stack — Démarrage"
echo "=========================================="

cd /workspaces/agent-monitoring-devops

echo ""
echo "1. Correction vm.max_map_count pour Elasticsearch..."
sudo sysctl -w vm.max_map_count=262144

echo ""
echo "2. Correction des permissions Filebeat..."
sudo chown root:root ./filebeat/filebeat.yml
sudo chmod go-w ./filebeat/filebeat.yml

echo ""
echo "3. Lancement des conteneurs..."
docker compose up -d --build

echo ""
echo "4. Attente du démarrage d'Elasticsearch (peut prendre 1-2 minutes)..."
until curl -s http://localhost:9200 > /dev/null 2>&1; do
    echo "   Elasticsearch pas encore prêt, on attend 10 secondes..."
    sleep 10
done
echo "   Elasticsearch est prêt !"

echo ""
echo "5. Attente du démarrage de Redis..."
until docker exec redis redis-cli ping > /dev/null 2>&1; do
    echo "   Redis pas encore prêt, on attend 5 secondes..."
    sleep 5
done
echo "   Redis est prêt !"

echo ""
echo "6. Vérification de tous les conteneurs..."
docker compose ps

echo ""
echo "7. Vérification des index Elasticsearch..."
sleep 10
curl -s http://localhost:9200/_cat/indices?v

echo ""
echo "8. Statistiques Redis..."
docker exec redis redis-cli KEYS "*" 2>/dev/null || echo "   Redis vide pour l'instant"

echo ""
echo "9. Exposition des ports publiquement..."
for port in 3000 5601 9090 8080 8000 5540; do
    gh codespace ports visibility ${port}:public \
        --codespace $CODESPACE_NAME 2>/dev/null \
        && echo "   Port $port public" \
        || echo "   Port $port erreur"
done

echo ""
echo "=========================================="
echo "  URLs de tes services :"
echo "=========================================="
echo "Kibana        : https://$CODESPACE_NAME-5601.app.github.dev"
echo "Grafana       : https://$CODESPACE_NAME-3000.app.github.dev/login"
echo "Prometheus    : https://$CODESPACE_NAME-9090.app.github.dev/graph"
echo "cAdvisor      : https://$CODESPACE_NAME-8080.app.github.dev/containers/"
echo "Replayer      : https://$CODESPACE_NAME-8000.app.github.dev/metrics"
echo "RedisInsight  : https://$CODESPACE_NAME-5540.app.github.dev"
echo "Identifiants Grafana : admin / admin"
echo ""
echo "=========================================="
echo "  Commandes utiles :"
echo "=========================================="
echo ""
echo "Voir les logs du replayer :"
echo "  docker compose logs -f log-replayer"
echo ""
echo "Lancer le collecteur Redis (dans un terminal séparé) :"
echo "  python3 /workspaces/agent-monitoring-devops/collector_redis.py"
echo ""
echo "Lancer le pipeline IA :"
echo "  python3 /workspaces/agent-monitoring-devops/ml_pipeline.py
        python3 /workspaces/agent-monitoring-devops/llm_analyzer_bgl.py"
echo ""
echo "Vérifier Redis :"
echo "  docker exec -it redis redis-cli"
echo ""
echo "Extraire les features pour le ML :"
echo "  python3 extract_features.py --output dataset_bgl_replay.csv"
echo ""
echo "Voir les anomalies rejouées :"
echo "  docker compose logs log-replayer | grep Anomalie"
echo "=========================================="
#bash /workspaces/agent-monitoring-devops/start.sh