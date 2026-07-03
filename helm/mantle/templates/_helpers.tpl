{{/*
Expand the name of the chart.
*/}}
{{- define "mantle.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "mantle.fullname" -}}
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

{{- define "mantle.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "mantle.labels" -}}
helm.sh/chart: {{ include "mantle.chart" . }}
{{ include "mantle.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "mantle.selectorLabels" -}}
app.kubernetes.io/name: {{ include "mantle.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "mantle.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "mantle.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "mantle.redisUrl" -}}
{{- if .Values.redis.enabled -}}
redis://{{ include "mantle.fullname" . }}-redis:{{ .Values.redis.service.port }}
{{- else -}}
{{ required "redis.url is required when redis.enabled is false" .Values.redis.url }}
{{- end -}}
{{- end }}

{{- define "mantle.rayAddress" -}}
ray://{{ include "mantle.fullname" . }}-ray-head:{{ .Values.ray.head.service.clientPort }}
{{- end }}

{{- define "mantle.vrpmSidecarUrl" -}}
{{- if .Values.config.analytics.vrpmSidecarUrl -}}
{{ .Values.config.analytics.vrpmSidecarUrl }}
{{- else -}}
http://{{ include "mantle.fullname" . }}-vrpm-sidecar:8090
{{- end -}}
{{- end }}

{{- define "mantle.configToml" -}}
[server]
bind = {{ .Values.config.server.bind | quote }}

[storage]
backend = {{ .Values.config.storage.backend | quote }}
bucket = {{ .Values.config.storage.bucket | quote }}
region = {{ .Values.config.storage.region | quote }}
{{- if .Values.config.storage.endpoint }}
endpoint = {{ .Values.config.storage.endpoint | quote }}
{{- end }}

[catalog]
postgres_url = {{ .Values.config.catalog.postgresUrl | quote }}
ducklake_data_path = {{ .Values.config.catalog.ducklakeDataPath | quote }}
geometry_column = {{ .Values.config.catalog.geometryColumn | quote }}

[cache]
redis_url = {{ include "mantle.redisUrl" . | quote }}
ifd_ttl_seconds = {{ .Values.config.cache.ifdTtlSeconds }}

[analytics]
broker = {{ .Values.config.analytics.broker | quote }}
stream_key = {{ .Values.config.analytics.streamKey | quote }}
ray_address = {{ include "mantle.rayAddress" . | quote }}
vrpm_sidecar_url = {{ include "mantle.vrpmSidecarUrl" . | quote }}
plugin_allowlist = {{ .Values.config.analytics.pluginAllowlist | default (list) | toJson }}

[auth]
admin_token_env = {{ .Values.config.auth.adminTokenEnv | quote }}
{{- end }}
