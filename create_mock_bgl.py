import os
import random
input_file = "d:/agent-devops/agent-monitoring-devops/data/BGL.log"
output_file = "d:/agent-devops/agent-monitoring-devops/data/BGL_mock_healthy.log"
normal_count = 0
anomaly_count = 0
TARGET_NORMAL = 5000
TARGET_ANOMALY = 5
with open(input_file, "r", encoding="utf-8", errors="replace") as fin, \
     open(output_file, "w", encoding="utf-8") as fout:
    
    for line in fin:
        is_normal = line.startswith("-")
        
        if is_normal and normal_count < TARGET_NORMAL:
            fout.write(line)
            normal_count += 1
        elif not is_normal and anomaly_count < TARGET_ANOMALY:
            # We add a random chance to not take the first 5 anomalies immediately
            if random.random() < 0.1:
                fout.write(line)
                anomaly_count += 1
                
        if normal_count >= TARGET_NORMAL and anomaly_count >= TARGET_ANOMALY:
            break
print(f"Fichier créé avec succès : {output_file}")
print(f"Lignes normales : {normal_count}")
print(f"Anomalies (critiques) : {anomaly_count}")
print(f"Taux d'anomalies : {(anomaly_count / (normal_count + anomaly_count)) * 100:.2f}%")
