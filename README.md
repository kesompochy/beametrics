# Beametrics

Let your logs be metrics in real-time with Apache Beam.

Beametrics transfers structured messages from a queue into metrics in real-time. Primarily designed to work with Cloud Pub/Sub to export metrics to Cloud Monitoring.

## Usage

### Direct Runner

```bash
$ python -m beametrics.main \
  --project=YOUR_PROJECT_ID \
  --subscription=projects/YOUR_PROJECT_ID/subscriptions/YOUR_SUBSCRIPTION \
  --metric-name=YOUR_METRIC_NAME \
  --metric-labels='{"LABEL": "HOGE"}' \
  --filter-conditions='[{"field": "user_agent", "value": "dummy_data", "operator": "equals"}]' \
  --runner=DirectRunner \
  --metric-type=count \
  --export-type=monitoring
```

### Dataflow Runner

#### 1. Build Docker image

```bash
$ docker build -t LOCATION-docker.pkg.dev/PROJECT_ID/REPOSITORY/beametrics:latest .
```

#### 2. Push Docker image to Artifact Registry

```bash
$ docker push LOCATION-docker.pkg.dev/PROJECT_ID/REPOSITORY/beametrics:latest
```

#### 3. Build Dataflow Flex Template

```bash
$ gcloud dataflow flex-template build gs://BUCKET/beametrics.json \
--image "LOCATION-docker.pkg.dev/PROJECT_ID/REPOSITORY/beametrics:latest" \
--sdk-language "PYTHON" \
--metadata-file "metadata.json"
```

#### 4. Run Dataflow job

```bash
$ cat flags.yaml
--parameters:
  project-id: YOUR_PROJECT_ID
  subscription: projects/YOUR_PROJECT_ID/subscriptions/YOUR_SUBSCRIPTION
  metric-name: YOUR_METRIC_NAME
  metric-labels: '{"LABEL": "HOGE"}'
  filter-conditions: '[{"field":"user_agent","value":"dummy_data","operator":"equals"}]'
  metric-type: count
  window-size: "120"
$ gcloud dataflow flex-template run "beametrics-job-$(date +%Y%m%d-%H%M%S)" \
--template-file-gcs-location gs://BUCKET/beametrics.json \
--region REGION \
--flags-file=flags.yaml
```

##### As a Dataflow Flex Template with external config file

```bash
 $ cat flags_external_config.yaml
--parameters:
  project-id: YOUR_PROJECT_ID
  subscription: projects/YOUR_PROJECT_ID/subscriptions/YOUR_SUBSCRIPTION
  window-size: "60"
  config: gs://YOUR_BUCKET/YOUR_CONFIG_FILE.yaml
$ cat YOUR_CONFIG_FILE.yaml
metrics:
  - name: beametrics-test-1
    labels:
      LABEL: HOGE1
    dynamic_labels:
      label_key: label_value
    filter-conditions:
      - field: user_agent
        value: dummy_data
        operator: equals
    type: count
    export_type: google-cloud-monitoring
  - name: beametrics-test-2
    labels:
      LABEL: HOGE2
    dynamic_labels:
      label_key: label_value
    filter-conditions:
      - field: user_agent
        value: dummy_data
        operator: equals
    type: count
    export_type: local
$ gcloud dataflow flex-template run "beametrics-job-$(date +%Y%m%d-%H%M%S)" \
--template-file-gcs-location gs://BUCKET/beametrics.json \
--region REGION \
--flags-file=flags_external_config.yaml
```
