{{/*
Naming and labelling, shared by every template so a resource cannot end up outside the
release's selector by accident.
*/}}

{{- define "dag-doctor.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "dag-doctor.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "dag-doctor.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "dag-doctor.labels" -}}
helm.sh/chart: {{ include "dag-doctor.chart" . }}
{{ include "dag-doctor.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: dag-doctor
{{- end }}

{{- define "dag-doctor.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dag-doctor.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* Per-component labels. The component label is what keeps the API and worker
Deployments from selecting each other's pods. */}}
{{- define "dag-doctor.componentLabels" -}}
{{ include "dag-doctor.labels" .root }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "dag-doctor.componentSelectorLabels" -}}
{{ include "dag-doctor.selectorLabels" .root }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "dag-doctor.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "dag-doctor.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "dag-doctor.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end }}

{{- define "dag-doctor.secretName" -}}
{{- default (printf "%s-secrets" (include "dag-doctor.fullname" .)) .Values.secrets.existingSecret }}
{{- end }}

{{/*
Environment shared by every container. Config comes from the ConfigMap and credentials
from the Secret, so a credential is never visible in a pod spec or in `kubectl describe`.
*/}}
{{- define "dag-doctor.env" -}}
envFrom:
  - configMapRef:
      name: {{ include "dag-doctor.fullname" . }}-config
  - secretRef:
      name: {{ include "dag-doctor.secretName" . }}
{{- end }}

{{/* The image runs read-only, so the two directories Python insists on writing are
mounted as memory-backed volumes rather than by relaxing the root filesystem. */}}
{{- define "dag-doctor.writableVolumes" -}}
- name: tmp
  emptyDir: {}
{{- end }}

{{- define "dag-doctor.writableVolumeMounts" -}}
- name: tmp
  mountPath: /tmp
{{- end }}
