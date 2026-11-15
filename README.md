# Invoice Pipeline : Big Data en streaming + MLOps

Pipeline de bout en bout qui génère des factures, les ingère en streaming, les stocke, entraîne un modèle de **prédiction de retard de paiement**, le versionne, puis le déploie sur Kubernetes en GitOps.

## Architecture

```
Générateur Python
   → Kafka (topic "invoices")
   → Spark Structured Streaming (console + Parquet)
   → HDFS (/data/invoices)
   → Spark ML / scikit-learn (entraînement)
   → MLflow (suivi + registre, artefacts sur MinIO)
   → alias "staging" / "production"
   → API FastAPI (image Docker)
   → Git → ArgoCD → Kubernetes (namespaces staging et preprod)

Airflow orchestre : check_data → train → train_sklearn → promote
```

## Services et ports

| Service | Rôle | Accès |
|---|---|---|
| Kafka | Ingestion des factures | `localhost:9092` |
| Spark | Streaming, entraînement | conteneur `spark` |
| HDFS (namenode, datanode) | Stockage distribué | http://localhost:9870 |
| MinIO | Stockage des modèles (S3) | http://localhost:9001 |
| MLflow | Suivi des expériences, registre | http://localhost:5000 |
| Airflow | Orchestration | http://localhost:8081 |
| API de prédiction (Docker) | Test local | http://localhost:8000/docs |
| ArgoCD | GitOps | https://localhost:8443 (port-forward) |

Les identifiants MinIO (`minioadmin` / `minioadmin123`) sont des valeurs de démonstration locales. Ne les réutilise pas ailleurs.

## Structure du dépôt

```
generator/        Producteur Python de factures (Faker + Kafka)
streaming/        Jobs Spark Structured Streaming
ml/               train.py (Spark ML), train_sklearn.py, promote.py
airflow/dags/     DAG invoice_late_payment_pipeline
serving/          API FastAPI (app.py) et son Dockerfile
k8s/              Manifests Kubernetes (staging, preprod) et applications ArgoCD
docker-compose.yml, Dockerfile*, init_bucket.py
```

## Prérequis

- Docker Desktop (16 Go de RAM conseillés, 15 Go de disque libre)
- Python 3.11+ avec `faker` et `confluent-kafka`
- `kind` et `kubectl` pour la partie Kubernetes

## Démarrage

### 1. Lancer l'infrastructure

```powershell
docker compose up -d --build
docker exec namenode hdfs dfs -mkdir -p /data/invoices
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic invoices --bootstrap-server localhost:9092
```

Attendre environ une minute que HDFS sorte du mode sans échec.

### 2. Générer et stocker des factures

Terminal 1 (job de streaming) :

```powershell
docker exec -it spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/streaming/stream_hdfs.py
```

Terminal 2 (générateur, laisser tourner 5 minutes environ) :

```powershell
pip install faker confluent-kafka
python generator/producer.py
```

Vérification :

```powershell
docker exec namenode hdfs dfs -count /data/invoices
```

### 3. Entraîner et enregistrer les modèles

Depuis Airflow (http://localhost:8081), activer le DAG `invoice_late_payment_pipeline` puis cliquer sur **Déclencher**. Il enchaîne :

1. `check_data` : présence de Parquet dans HDFS
2. `train` : Random Forest Spark ML (Pipeline `VectorAssembler` + forêt), modèle `invoice_late_payment`
3. `train_sklearn` : Random Forest scikit-learn, modèle `invoice_late_payment_sk` (servi par l'API)
4. `promote` : pose l'alias `staging` sur la dernière version de chaque modèle

Résultat attendu : AUC d'environ 0,64 (le retard est généré avec une part importante de hasard).

### 4. Tester l'API en local

```powershell
Invoke-RestMethod -Uri http://localhost:8000/predict -Method Post -ContentType "application/json" -Body '{"amount":4500,"due_days":15,"issue_month":6,"cust_late_rate":0.3,"cust_nb_invoices":300}'
```

### 5. Déployer sur Kubernetes (kind + ArgoCD)

```powershell
kind create cluster --name invoice
kubectl create namespace staging
kubectl create namespace preprod
kind load docker-image invoice-serving:latest --name invoice
docker network connect invoice-pipeline_default invoice-control-plane

kubectl create namespace argocd
kubectl apply -n argocd --server-side --force-conflicts -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
kubectl apply -f k8s/argocd-apps.yaml
```

Mot de passe initial d'ArgoCD (login `admin`) :

```powershell
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | ForEach-Object { [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($_)) }
kubectl port-forward -n argocd svc/argocd-server 8443:443
```

Chaque `git push` sur `k8s/` est ensuite synchronisé automatiquement vers le cluster (vérification toutes les 3 minutes environ).

### 6. Promouvoir un modèle vers pre-prod

```powershell
docker exec spark python3 -u -c "import mlflow; mlflow.set_tracking_uri('http://mlflow:5000'); from mlflow import MlflowClient; MlflowClient().set_registered_model_alias('invoice_late_payment_sk','production','1')"
```

Puis dans `k8s/preprod/deployment.yaml`, `MODEL_URI` vaut `models:/invoice_late_payment_sk@production`, et un `git push` déclenche le redéploiement. Staging continue de servir `@staging`.

## Reprendre après un arrêt de Docker

```powershell
docker compose up -d
docker start invoice-control-plane
docker network connect invoice-pipeline_default invoice-control-plane   # "already exists" est normal
kubectl get applications -n argocd
```

Les ports-forward sont à relancer.

## Limites connues

- Le cluster kind charge l'image de l'API par `kind load` : après un rebuild de `invoice-serving`, relancer le `kind load` puis `kubectl rollout restart`.
- Airflow pilote Spark et HDFS via le socket Docker (`docker exec`) : adapté à un environnement local, pas à de la production.
- Les données sont synthétiques : l'AUC reflète le générateur, pas un cas réel.
- Kubeflow et le notebook de l'analyste ne sont pas implémentés.
