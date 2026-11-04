# Kafka Crypto Ticker

Binance aggregate trade ingester project to learn event streaming, Microservices architecture, Kubernetes, and CI/CD practices using Github Actions and ArgoCD

## Description

The initial scope of the project was to learn event streaming with Kafka, but the distributed nature of the app was also a good fit to learn Microservices, Kubernetes, and CI/CD. 

This project includes:
- An ingester to take in live Binance aggTrade data.
- An archiver that stores & compacts trades into parquet files into Rustfs buckets.
- A service that inserts and dedupes trades to store in a Clickhouse database.
- A web API that queries historical candlestick data and provides a Websocket for the current candlesticks.


## Getting Started

### Dependencies

* Dotnet 10.0 SDK
* Minikube
* Docker (For running containers locally)

### Installing
* Clone the repo
```bash
gh repo clone pepis2317/kafka-crypto-ticker
```
* Import the project to your favorite IDE, then install dependencies
```bash
dotnet restore
```
* Initialize secrets
```bash
minikube start

kubectl create secret generic clickhouse-secrets -n dev \
  --from-literal=ch_password=<your-ch-password> \

kubectl create secret generic grafana-secrets -n dev \
  --from-literal=admin_user=<your-admin-user> \
  --from-literal=admin_password=<your-admin-password>

kubectl create secret generic redis-secrets -n dev \
  --from-literal=redis_password=<your-redis-password>

kubectl create secret generic rustfs-secrets -n dev \
  --from-literal=rustfs_access_key=<your-access-key> \
  --from-literal=rustfs_secret_key=<your-secret-key>

kubectl create secret generic grafana-secrets -n dev \
  --from-literal=admin_user=<your-admin-user> \
  --from-literal=admin_password=<your-admin-password>
```
* Apply dev deployment manually to Minikube
```bash
kubectl apply -k Kubernetes/base/dev/
kubectl get po -n dev
```
### Running Grafana
* Run the Grafana NodePort service
```bash
minikube service grafana -n dev
```

### Installing ArgoCD
* Create namespace
```bash
kubectl create ns argocd
```
* Apply ArgoCD manifest installation file
```bash
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/v2.5.8/manifests/install.yaml

kubectl get all -n argocd
```
* Port forward ArgoCD UI
```bash
kubectl port-forward svc/argocd-server -n argocd 8080:443
```
* In another terminal, view the default password
```bash
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d; echo
```
* Open localhost:8080 n your browser and log in as admin
```text
Username: admin
Password: password-from-previous-step
```

### Deploying the app in ArgoCD
* Open settings
* In settings, click on Repositories
* In settings/repos, click Connect Repo
* Set HTTPS as the connection method, use default project, and copy the repo URL, then hit Connect
```text
Choose your connection method: VIA HTTPS
Project: default
Repository URL: https://github.com/pepis2317/kafka-crypto-ticker.git
```
* CLick on the 3 dots on the repo and hit Create Application
* Enter general information like application name, project name, define sync policy, then select the path
```text
Application Name: kafka-crypto-ticker
Project Name: default
Sync Policy: Manual
...
Path: Kubernetes/base/dev/
```
* Hit Create and you should see the app

### Running locally with Docker Compose
For example, if you just want to start the ingester

* Go into the ingester directory
```bash
cd src/crypto-ticker.Ingester
```
* View project dependencies
```bash
dotnet list package
```
```text
Restore complete (3.4s)

Build succeeded in 4.2s
Project 'crypto-ticker.Ingester' has the following package references
   [net10.0]:
   Top-level Package                   Requested               Resolved
   > Confluent.Kafka                   2.16.0                  2.16.0
   > Microsoft.Extensions.Hosting      11.0.0-rc.2.26504.105   11.0.0-rc.2.26504.105
```
* Run appropriate containers listed in compose.yaml
```bash
docker compose up -d kafka
```
* Run the app
```bash
dotnet run
```

## What I would include in the future
* Add a functional Loki dashboard to my Grafana manifest
* Deploying to a cloud provider like Oracle
* Comprehensive unit test coverage
* A front end app to visualize aggregate trade candlesticsk
