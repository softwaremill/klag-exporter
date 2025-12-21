# Local development of helm chart

## Create a local kubernetes cluster

There are several ways to create a local kubernetes cluster. Some popular options include:
- [Minikube](https://minikube.sigs.k8s.io/docs/start/)
- [Kind](https://kind.sigs.k8s.io/docs/user/quick-start/)
- [K3d](https://k3d.io/#installation)
- [MicroK8s](https://microk8s.io/docs)
- [Docker Desktop](https://docs.docker.com/desktop/kubernetes/)

Use cluster of you choice. For example, to create a k3d cluster, run:
```bash
k3d cluster create klog-test --agents 3
```

## Install kube-prometheus-stack

To install the kube-prometheus-stack helm chart, run the following commands:
```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm install k8s-test-stack prometheus-community/kube-prometheus-stack --namespace monitoring --create-namespace

```

## Install strimzi-kafka-operator

To install the strimzi-kafka-operator helm chart, run the following commands:
```bash
helm repo add strimzi https://strimzi.io/charts/
helm repo update
helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator --namespace strimzi --create-namespace --set watchAnyNamespace=true
```

## Create simple kafka cluster

To create a simple kafka cluster using strimzi, run the following command:
```bash
kubectl create namespace kafka
kubectl apply -f https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/refs/heads/main/examples/kafka/kafka-single-node.yaml -n kafka
```

## Deploy test producer and consumer

To test your Kafka cluster and generate consumer lag, deploy a test producer and consumer:

```bash
# Deploy the producer (sends messages every 1 second)
kubectl apply -f test-producer.yaml

# Deploy the consumer (processes messages with 10 second delay to create lag)
kubectl apply -f test-consumer.yaml

# Check producer logs
kubectl logs -f kafka-test-producer -n kafka

# Check consumer logs
kubectl logs -f kafka-test-consumer -n kafka

# Clean up
kubectl delete -f test-producer.yaml
kubectl delete -f test-consumer.yaml
```

## Deploy klag-exporter helm chart

To deploy the klag-exporter helm chart, run the following command:
```bash
helm upgrade --install klag-exporter ../klag-exporter -f test-values.yaml -n kafka
```
