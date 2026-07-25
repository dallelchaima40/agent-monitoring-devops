import logging
import time
import random
import redis
import os

HOSTNAME = os.uname()[1]

class HostnameFilter(logging.Filter):
    def filter(self, record):
        record.hostname = HOSTNAME
        return True

logger = logging.getLogger("app-python")
logger.addFilter(HostnameFilter())

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    '%(asctime)s %(hostname)s %(name)s[%(process)d]: %(levelname)s %(message)s'
))
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)
def connect_redis():
    try:
        r = redis.Redis(host='redis', port=6379)
        r.ping()
        logger.info("Connexion Redis établie avec succès")
        return r
    except Exception as e:
        logger.error(f"Impossible de se connecter à Redis : {e}")
        return None

def simulate_activity(r):
    users = ["root", "admin", "user1", "guest", "test"]
    ips = ["192.168.1.10", "10.0.0.5", "172.16.0.3", "192.168.0.254"]
    services = ["mysql", "postgresql", "mongodb", "redis"]
    ports = [3306, 5432, 27017, 6379]
    pages = ["/index.html", "/api/users", "/api/data", "/dashboard", "/login", "/health"]
    methods = ["GET", "POST", "PUT", "DELETE"]

    # Logs ERROR et WARN — situations anormales
    actions_error = [
        (lambda: f"authentication failure; user={random.choice(users)} rhost={random.choice(ips)} uid={random.randint(0,1000)}", "ERROR"),
        (lambda: f"connection timeout from {random.choice(ips)} to service {random.choice(services)} port={random.choice(ports)}", "ERROR"),
        (lambda: f"Failed password for {random.choice(users)} from {random.choice(ips)} port {random.randint(1024,65535)}", "ERROR"),
        (lambda: f"Out of memory: Kill process {random.randint(100,9999)} total-vm:{random.randint(100,9999)}kB", "ERROR"),
        (lambda: f"connection refused from {random.choice(ips)} port {random.randint(1024,65535)}", "ERROR"),
        (lambda: f"disk I/O error on device /dev/sd{random.choice('abcd')} sector {random.randint(1000,99999)}", "ERROR"),
        (lambda: f"CPU usage exceeded 90% for process {random.randint(100,9999)} ({random.choice(services)})", "WARN"),
        (lambda: f"High memory usage detected: {random.randint(80,95)}% of total RAM consumed", "WARN"),
        (lambda: f"Slow query detected on {random.choice(services)}: {random.randint(2000,9999)}ms execution time", "WARN"),
        (lambda: f"Disk space warning: /dev/sd{random.choice('abcd')} at {random.randint(80,95)}% capacity", "WARN"),
    ]

    # Logs INFO — situations normales
    actions_normal = [
        (lambda: f"session opened for user {random.choice(users)} by uid={random.randint(0,1000)}", "INFO"),
        (lambda: f"session closed for user {random.choice(users)}", "INFO"),
        (lambda: f"Received SNMP packet from {random.choice(ips)} community public", "INFO"),
        (lambda: f"HTTP {random.choice(methods)} {random.choice(pages)} from {random.choice(ips)} - 200 OK in {random.randint(10,500)}ms", "INFO"),
        (lambda: f"HTTP {random.choice(methods)} {random.choice(pages)} from {random.choice(ips)} - 200 OK in {random.randint(10,500)}ms", "INFO"),
        (lambda: f"HTTP {random.choice(methods)} {random.choice(pages)} from {random.choice(ips)} - 200 OK in {random.randint(10,500)}ms", "INFO"),
        (lambda: f"Database connection pool: {random.randint(1,10)}/20 connections active", "INFO"),
        (lambda: f"Cache hit ratio: {random.randint(85,99)}% for {random.choice(services)}", "INFO"),
        (lambda: f"Backup completed successfully for database {random.choice(services)}", "INFO"),
        (lambda: f"Health check passed for service {random.choice(services)} on port {random.choice(ports)}", "INFO"),
        (lambda: f"User {random.choice(users)} logged in successfully from {random.choice(ips)}", "INFO"),
        (lambda: f"Scheduled job completed: database cleanup took {random.randint(1,30)}s", "INFO"),
        (lambda: f"SSL certificate valid for {random.randint(30,365)} days", "INFO"),
        (lambda: f"API rate limit: {random.randint(1,100)}/1000 requests used from {random.choice(ips)}", "INFO"),
        (lambda: f"Service {random.choice(services)} restarted successfully after update", "INFO"),
    ]

    while True:
        # 75% de chance de log normal, 25% de log erreur
        if random.random() < 0.75:
            action, level = random.choice(actions_normal)
        else:
            action, level = random.choice(actions_error)

        message = action()

        if level == "INFO":
            logger.info(message)
        elif level == "WARN":
            logger.warning(message)
        elif level == "ERROR":
            logger.error(message)

        if r:
            try:
                r.lpush("logs_queue", message)
            except:
                pass

        time.sleep(random.uniform(1, 5))

if __name__ == "__main__":
    logger.info("Démarrage de l'application Python")
    r = connect_redis()
    simulate_activity(r)