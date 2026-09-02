#!/usr/bin/env bash
set -euo pipefail

: "${GH_TOKEN:?GH_TOKEN is required}"
: "${REPO:?REPO is required}"

retry_count="${CI_LOG_RETRY_COUNT:-3}"
retry_delay="${CI_LOG_RETRY_DELAY_SECONDS:-2}"
per_job_bytes="${CI_LOG_PER_JOB_BYTES:-14000}"

is_blob_not_found() {
  grep -aEq '<Code>[[:space:]]*BlobNotFound[[:space:]]*</Code>' "$1"
}

download_job_log() {
  local job_id="$1" destination="$2" attempt=1
  while [ "$attempt" -le "$retry_count" ]; do
    # Each request starts at the GitHub API endpoint so a stale Azure redirect is
    # never reused. The response body is kept private until it is validated.
    if curl -L --fail-with-body --silent --show-error \
      -H "Authorization: Bearer $GH_TOKEN" \
      -H 'Accept: application/vnd.github+json' \
      -H 'X-GitHub-Api-Version: 2022-11-28' \
      --output "$destination" \
      "https://api.github.com/repos/$REPO/actions/jobs/$job_id/logs" &&
      ! is_blob_not_found "$destination"; then
      return 0
    fi
    : >"$destination"
    if [ "$attempt" -lt "$retry_count" ] && [ "$retry_delay" != 0 ]; then
      sleep "$retry_delay"
    fi
    attempt=$((attempt + 1))
  done
  return 1
}

select_failure_window() {
  local source="$1" destination="$2"
  awk '
    { lines[NR]=$0 }
    !found && ($0 ~ /##\[error\]/ || tolower($0) ~ /(^|[^[:alnum:]_])(error|fatal|failed|failure)([^[:alnum:]_]|$)/) {
      marker=NR; found=1
    }
    END {
      if (!found) marker=NR
      start=marker-40; if (start < 1) start=1
      finish=marker+80; if (finish > NR) finish=NR
      for (i=start; i<=finish; i++) print lines[i]
    }
  ' "$source" >"$destination"

  if [ "$(wc -c <"$destination")" -gt "$per_job_bytes" ]; then
    local marker_line prefix suffix
    marker_line="$(grep -aniEm1 '##\[error\]|(^|[^[:alnum:]_])(error|fatal|failed|failure)([^[:alnum:]_]|$)' "$destination" | cut -d: -f1 || true)"
    marker_line="${marker_line:-1}"
    prefix="$(mktemp)"; suffix="$(mktemp)"
    sed -n "1,$((marker_line - 1))p" "$destination" | tail -c 6000 >"$prefix"
    sed -n "$((marker_line + 1)),\$p" "$destination" | head -c 6000 >"$suffix"
    {
      cat "$prefix"
      sed -n "${marker_line}p" "$destination" | head -c 1800
      cat "$suffix"
    } >"$destination.compact"
    mv "$destination.compact" "$destination"
    rm -f "$prefix" "$suffix"
  fi
}

jobs_file="$(mktemp)"
trap 'rm -f "$jobs_file" "${raw_log:-}" "${window:-}"' EXIT
cat >"$jobs_file"

failed_jobs="$(jq -r '[.jobs[] | select(.conclusion == "failure") | .name] | if length == 0 then ["workflow failed without a failed job"] else . end | join("\n")' "$jobs_file")"
printf 'Failed jobs:\n%s' "$failed_jobs"

while IFS= read -r failed_job; do
  job_id="$(jq -r '.id' <<<"$failed_job")"
  job_name="$(jq -r '.name' <<<"$failed_job")"
  raw_log="$(mktemp)"; window="$(mktemp)"
  printf '\n\n=== %s (failure context, max %s bytes) ===\n' "$job_name" "$per_job_bytes"
  if download_job_log "$job_id" "$raw_log"; then
    select_failure_window "$raw_log" "$window"
    cat "$window"
  else
    jq -nc --argjson job_id "$job_id" --arg job_name "$job_name" --argjson attempts "$retry_count" \
      '{ci_log:{status:"unavailable",reason:"job_log_retrieval_failed",job_id:$job_id,job_name:$job_name,attempts:$attempts}}'
  fi
  rm -f "$raw_log" "$window"
  raw_log=""; window=""
done < <(jq -c '.jobs[] | select(.conclusion == "failure") | {id, name}' "$jobs_file")
