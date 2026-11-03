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
```
gh repo clone pepis2317/kafka-crypto-ticker
```
* Import the project to your favorite IDE, then install dependencies
```
dotnet restore
```
* Start Minikube
```
minikube start
```
* Apply dev deployment manually to Minikube
```
kubectl apply -k Kubernetes/base/dev/
```

### Executing program
```
minikube start
```



## What I would include in the future
* Add a functional Loki dashboard to my Grafana manifest
* Deploying to a cloud provider like Oracle
* Comprehensive unit test coverage
* A front end app to visualize aggregate trade candlesticsk
