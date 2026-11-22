#!/usr/bin/env bash
# Checks that the deployment is working end to end. Exits non-zero with the reason when not.
# shellcheck source=common.sh source-path=SCRIPTDIR
source "$(dirname "$(realpath "$0")")/common.sh"
trap - ERR

problems=()
check() { "$@" >/dev/null 2>&1 || problems+=("${CHECK_LABEL:-$*}"); }

for unit in "$POD_SERVICE" "$DB_SERVICE" "$APP_SERVICE" "${TIMERS[@]}"; do
    CHECK_LABEL="$unit is not active" check systemctl --user is-active --quiet "$unit"
done
# A failing backfill leaves files waiting in the inbox while the timer stays active.
if [[ -n $SECONDARY_HOST ]] && systemctl --user is-failed --quiet "$NAME-pull.service"; then
    problems+=("the last pull from the secondary collector failed; see journalctl --user -u $NAME-pull.service")
fi
CHECK_LABEL='collector healthcheck fails' check podman healthcheck run "$APP_CONTAINER"
CHECK_LABEL='Postgres is not ready' check podman exec "$DB_CONTAINER" pg_isready -q -h /var/run/postgresql -U collector -d skyblock

status=$(curl -fsS -m 5 "$STATUS_URL" 2>/dev/null || true)
if [[ -z $status ]]; then
    problems+=("status server $STATUS_URL is unreachable")
else
    jq -e '.ingest.database_ok' <<<"$status" >/dev/null || problems+=("database writes failing: $(jq -r '.ingest.last_database_error' <<<"$status")")
    jq -e '.ingest.pending_jobs < 20' <<<"$status" >/dev/null || problems+=("$(jq -r '.ingest.pending_jobs' <<<"$status") jobs waiting in the spool")
    jq -e '.archive_errors == 0' <<<"$status" >/dev/null || problems+=("archive write errors: $(jq -r '.last_archive_error' <<<"$status")")
    for feed in auctions_ended auctions bazaar; do
        age=$(jq -r --arg f "$feed" '.feeds[$f].last_new_snapshot_at // empty | sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601 | now - . | floor' <<<"$status")
        [[ -n $age && $age -lt 300 ]] || problems+=("$feed has no new snapshot for ${age:-ever} s")
    done
fi

recent=$(psql_q -c "SELECT count(*) FROM sales WHERE collected_at > now() - interval '10 minutes'" 2>/dev/null || echo 0)
((recent > 0)) || problems+=('no sales written in the last 10 minutes')
free_gb=$(($(df --output=avail -k "$APP" | tail -1) / 1024 / 1024))
((free_gb >= 60)) || problems+=("only $free_gb GB free on the disk")
last_point=$(find "$BACKUPS" -maxdepth 1 -type d -name 'daily-*' -printf '%T@ %f\n' | sort -n | tail -1 | cut -d' ' -f2)
# The secondary finishes a file per feed each hour, and the pull runs every 5 minutes. A file keeps
# its time from the secondary, when it was last written.
secondary='no secondary collector'
if [[ -n $SECONDARY_HOST ]]; then
    newest=$(find "$SECONDARY_DATA" -type f -name '*.ndjson.zst' -printf '%T@\n' 2>/dev/null | sort -n | tail -1 | cut -d. -f1 || true)
    if [[ -z $newest ]]; then
        problems+=("no file from the secondary collector on $SECONDARY_HOST yet")
    else
        secondary_age=$(($(date +%s) - newest))
        ((secondary_age < 7800)) ||
            problems+=("no new file from the secondary collector on $SECONDARY_HOST for $((secondary_age / 60)) min")
        secondary="newest secondary file $((secondary_age / 60)) min old"
    fi
fi

if ((${#problems[@]})); then
    printf 'NOT OK: %s\n' "${problems[@]}"
    exit 1
fi
printf 'OK: collecting (%s sales in 10 min), %s GB free, newest daily restore point: %s, %s\n' \
    "$recent" "$free_gb" "${last_point:-none yet}" "$secondary"
