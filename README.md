# Beametrics

Let your logs be metrics in real-time with Apache Beam.

## Usage

### Direct Runner

```bash
$ python -m beametrics.main \
  --project=YOUR_PROJECT_ID \
  --subscription=projects/YOUR_PROJECT_ID/subscriptions/YOUR_SUBSCRIPTION \
  --export-metric-name=YOUR_METRIC_NAME \
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
  export-metric-name: YOUR_METRIC_NAME
  metric-labels: '{"LABEL": "HOGE"}'
  filter-conditions: '[{"field":"user_agent","value":"bigfoot_dummy_data","operator":"equals"}]'
  metric-type: count
  window-size: "120"
$ gcloud dataflow flex-template run "beametrics-job-$(date +%Y%m%d-%H%M%S)" \
--template-file-gcs-location gs://BUCKET/beametrics.json \
--region REGION \
--flags-file=flags.yaml
```
