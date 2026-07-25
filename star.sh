#!/bin/bash

echo "Démarrage du projet agent-monitoring-devops..."
cd /workspaces/agent-monitoring-devops

echo "Correction des permissions Filebeat..."
sudo chown root:root /workspaces/agent-monitoring-devops/filebeat/filebeat.yml

echo "Lancement de tous les conteneurs..."
docker compose up -d

echo "Attente du démarrage des conteneurs (30 secondes)..."
sleep 30

echo "Arrêt de Kibana pour libérer de la mémoire..."
docker compose stop kibana

echo "Redémarrage de Filebeat..."
docker compose restart filebeat

echo "Attente qu'Elasticsearch soit prêt..."
until curl -s http://localhost:9200 > /dev/null; do
  echo "Elasticsearch pas encore prêt, on attend..."
  sleep 10
done

echo "Elasticsearch est prêt !"
echo "Installation des dépendances Python dans le conteneur..."
docker exec app-python pip install requests elasticsearch

echo "Copie du collecteur dans le conteneur..."
docker cp /workspaces/agent-monitoring-devops/app-python/collector.py app-python:/app/collector.py

echo "Vérification des index Elasticsearch..."
curl http://localhost:9200/_cat/indices?v

echo "Tout est prêt. Lance le collecteur avec cette commande :"
echo "docker exec -it app-python python3 /app/collector.py"
echo "Grafana       : https://$CODESPACE_NAME-3000.app.github.dev/login"
echo "Prometheus    : https://$CODESPACE_NAME-9090.app.github.dev"
echo "Redis Insight : https://$CODESPACE_NAME-5540.app.github.dev"

