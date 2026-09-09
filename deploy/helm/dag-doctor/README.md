# dag-doctor Helm chart

Deploys the API and the worker as separate Deployments, so they scale independently: the
API scales on request load, the worker on how far behind the failure topic it is.

```bash
helm upgrade --install dag-doctor deploy/helm/dag-doctor \
  --namespace dag-doctor --create-namespace \
  -f deploy/helm/dag-doctor/values-prod.yaml
```

For a local cluster, `make kind-deploy` creates a kind cluster, deploys the dependencies
in [`deploy/kind/dependencies.yaml`](../../kind/dependencies.yaml), pulls a small model,
loads the image, and installs the chart.

## What it deploys

| Resource | Notes |
|---|---|
| `Deployment/…-api` | FastAPI, probes on the real `/health/live` and `/health/ready` |
| `Deployment/…-worker` | Kafka consumer running the investigation graph |
| `Job/…-migrate` | `alembic upgrade head`, as a `pre-install,pre-upgrade` hook |
| `ConfigMap`, `Secret` | Non-secret config and credentials, kept apart |
| `HorizontalPodAutoscaler` | Worker on consumer lag, API on CPU. Both off by default |
| `PodDisruptionBudget` | One per component |
| `ServiceMonitor` | Two, because the processes have separate registries |

## Three decisions worth knowing about

### The worker has no preStop hook, on purpose

The worker finishes the incident it is holding when it receives `SIGTERM`: it stops taking
new records, drains the one in flight, and exits. What makes that work is
`terminationGracePeriodSeconds`, which defaults to 900 so it comfortably exceeds one
investigation.

A `preStop` hook would not help here and would actively hurt. `preStop` runs *before*
`SIGTERM` and inside the same grace period, so a sleep there delays the drain and shortens
the window the drain has to complete in.

The API does have one, where it is the right tool: it gives the endpoints controller time
to withdraw the pod before it stops serving, so a rollout does not drop requests that were
already routed to it.

### The worker HPA needs a metrics provider

Scaling a worker on CPU is close to meaningless: it spends its time waiting on a model, so
utilisation stays low while incidents queue up. Consumer lag is the metric that matters.

Kubernetes cannot read that on its own. The HPA references
`kafka_consumergroup_lag` as an **external metric**, which needs one of:

- **Prometheus Adapter**, configured to expose that series as an external metric, with a
  Kafka lag exporter feeding it, or
- **KEDA**, whose `kafka` scaler does this directly and is the simpler option if you are
  not already running the adapter.

Without one the HPA reports its target as `<unknown>` and never scales, and says nothing
about why. That is why `worker.autoscaling.enabled` is `false` by default: an autoscaler
that looks configured but silently does nothing is worse than no autoscaler.

Cap `maxReplicas` at the failure topic's partition count. A consumer group cannot have
more active members than partitions, so anything above that is idle pods.

### Committed secrets are for local use only

`secrets.create: true` renders a `Secret` from values, which is what makes a local install
work without ceremony. It is not a production pattern: values files end up in git, and git
history is forever.

For production, set `secrets.create: false` and point `secrets.existingSecret` at a Secret
managed by [External Secrets](https://external-secrets.io/), SOPS, or your cloud's secret
manager. `values-prod.yaml` does this, and the chart's `NOTES.txt` warns when a prod-mode
install is still reading credentials from values.

## Testing changes

```bash
make helm-lint      # lint and render against all three values files
```

CI additionally validates the rendered manifests against the real Kubernetes API schemas
with `kubeconform`, which catches misspelled fields that `helm lint` accepts and a cluster
would silently ignore.
