import logging
import time
import random
import redis
import os

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(hostname)s %(name)s[%(process)d]: %(levelname)s %(message)s'
)

class HostnameFilter(logging.Filter):
    hostname = os.uname()[1]
    def filter(self, record):
        record.hostname = self.hostname
        return True

logger = logging.getLogger("app-python")
logger.addFilter(HostnameFilter())

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

    actions = [
        lambda: f"authentication failure; user={random.choice(users)} rhost={random.choice(ips)} uid={random.randint(0,1000)}",
        lambda: f"connection timeout from {random.choice(ips)} to service {random.choice(services)} port={random.choice(ports)}",
        lambda: f"session opened for user {random.choice(users)} by uid={random.randint(0,1000)}",
        lambda: f"session closed for user {random.choice(users)}",
        lambda: f"Failed password for {random.choice(users)} from {random.choice(ips)} port {random.randint(1024,65535)}",
        lambda: f"Out of memory: Kill process {random.randint(100,9999)} total-vm:{random.randint(100,9999)}kB",
        lambda: f"connection refused from {random.choice(ips)} port {random.randint(1024,65535)}",
        lambda: f"disk I/O error on device /dev/sd{random.choice('abcd')} sector {random.randint(1000,99999)}",
        lambda: f"CPU usage exceeded 90% for process {random.randint(100,9999)} ({random.choice(services)})",
        lambda: f"Received SNMP packet from {random.choice(ips)} community public",
    ]

    levels = ["ERROR", "ERROR", "INFO", "INFO", "ERROR", "ERROR", "WARN", "ERROR", "WARN", "INFO"]

    while True:
        idx = random.randint(0, len(actions)-1)
        message = actions[idx]()
        level = levels[idx]

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

        time.sleep(random.uniform(0.5, 2))

if __name__ == "__main__":
    logger.info("Démarrage de l'application Python")
    r = connect_redis()
    simulate_activity(r)