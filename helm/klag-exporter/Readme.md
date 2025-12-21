# Helm Chart for klag-exporter

Helm chart for deploying klag-exporter to Kubernetes.

## Installation

### From OCI Registry (GitHub Container Registry)

```bash
# Install the latest version
helm install klag-exporter oci://ghcr.io/softwaremill/helm/klag-exporter

# Install a specific version
helm install klag-exporter oci://ghcr.io/softwaremill/helm/klag-exporter --version 0.1.1

# Install with custom values
helm install klag-exporter oci://ghcr.io/softwaremill/helm/klag-exporter \
  --set config.clusters[0].bootstrap_servers="kafka:9092" \
  --set config.clusters[0].name="my-cluster"

# Install with values file
helm install klag-exporter oci://ghcr.io/softwaremill/helm/klag-exporter \
  -f custom-values.yaml \
  -n kafka --create-namespace
```

### From Source

```bash
# Clone the repository
git clone https://github.com/softwaremill/klag-exporter.git
cd klag-exporter/helm

# Install from local chart
helm install klag-exporter ./klag-exporter -f custom-values.yaml -n kafka --create-namespace
```

## Configuration

See `values.yaml` for all available configuration options. Key configurations:

### Basic Configuration

```yaml
config:
  exporter:
    poll_interval: "30s"
    http_port: 8000
    granularity: "topic"  # or "partition"

  clusters:
    - name: "my-cluster"
      bootstrap_servers: "kafka:9092"
      group_whitelist: [".*"]
      topic_blacklist: ["^__.*"]
```

### Authentication (SASL/SSL)

```yaml
config:
  clusters:
    - name: "production"
      bootstrap_servers: "kafka:9092"
      clusters.consumer_properties:
        security.protocol: "SASL_SSL"
        sasl.mechanism: "PLAIN"
        sasl.username: "${KAFKA_USER}"
        sasl.password: "${KAFKA_PASSWORD}"
```

### Prometheus ServiceMonitor

```yaml
serviceMonitor:
  enabled: true
  additionalEndpointConfig:
    interval: 30s
```

### Custom Secret Volumes

Mount Kafka credentials or SSL certificates:

```yaml
customSecretVolumes:
  - name: kafka-certs
    secretName: kafka-ssl-certs
    mountPath: /etc/kafka/certs
    readOnly: true
```

## Upgrading

```bash
# Upgrade from OCI registry
helm upgrade klag-exporter oci://ghcr.io/softwaremill/helm/klag-exporter --version 0.1.2

# Upgrade from local chart
helm upgrade klag-exporter ./klag-exporter -f custom-values.yaml
```

## Uninstalling

```bash
helm uninstall klag-exporter -n kafka
```
