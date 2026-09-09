{{/* The chart's base name, overridable. */}}
{{- define "dhole.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* The release-qualified name every object is prefixed with. */}}
{{- define "dhole.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "dhole.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "dhole.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{- define "dhole.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "dhole.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
The image reference. An empty tag falls back to the chart's appVersion rather
than to "latest": a floating tag would make two installs of one chart version
run different code, and every cache key folded over it a lie.
*/}}
{{- define "dhole.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- printf "%s/%s:%s" .Values.image.registry .Values.image.repository $tag -}}
{{- end -}}

{{/* The NATS URL the plane and every engine dial. */}}
{{- define "dhole.busURL" -}}
{{- if .Values.nats.external -}}
{{- .Values.nats.external -}}
{{- else -}}
{{- printf "nats://%s-nats:4222" (include "dhole.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/* The name of the secret holding the store DSN. */}}
{{- define "dhole.storeSecretName" -}}
{{- default (printf "%s-store" (include "dhole.fullname" .)) .Values.postgres.existingSecret -}}
{{- end -}}
