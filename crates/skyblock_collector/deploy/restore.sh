#!/usr/bin/env bash
# Restores the database. Usage:
#   restore.sh <restore point directory>   restore from a backup, then replay newer archive files
#   restore.sh --rebuild [revision]         rebuild from the raw archive alone, optionally with
#                                           the image of a release that changes the schema
#
# A revision is a release tag such as v0.3.0, or the commit of one. The one unreleased revision
# it takes is the one in pre-trellis-rev, which ran before the first release, and only while its
# image is kept as :pre-trellis.
#
# Builds the new copy in a separate database and replays the archive into it while the collector
# keeps writing to the current one, so history goes in oldest first. The collector stops only to
# replay what it archived during the last pass, which took under a minute, and to swap. The replaced database is kept as
# skyblock_before_restore_<time>; drop it once the result looks right.
# shellcheck source=common.sh source-path=SCRIPTDIR
source "$(dirname "$(realpath "$0")")/common.sh"

usage='usage: restore.sh <restore point directory> | restore.sh --rebuild [revision]'
point='' image=''
if [[ ${1:-} == --rebuild ]]; then
    revision=${2:-}
else
    point=$(realpath "${1:?$usage}")
    [[ $point == "$BACKUPS"/* && -d $point && -f $point/SHA256SUMS ]] || fail "not a restore point under $BACKUPS: $point"
fi
pre_release=$(cat "$APP/pre-trellis-rev" 2>/dev/null || true)
is_pre_release() { [[ -n $pre_release && ${#revision} -ge 7 && $pre_release == "$revision"* ]]; }
# podman image exists exits 1 for a missing image; any other failure is podman's own.
pre_release_image_kept() {
    local status=0
    [[ -n $pre_release ]] || return 1
    run podman image exists "$IMAGE:$PRE_RELEASE_TAG" || status=$?
    ((status <= 1)) || fail "could not check for $IMAGE:$PRE_RELEASE_TAG (podman exit $status)"
    return "$status"
}
if [[ -n ${revision:-} ]] && is_pre_release && ! pre_release_image_kept; then
    fail "the image of $pre_release, $IMAGE:$PRE_RELEASE_TAG, was removed, so that revision can no longer be rebuilt; pass a release tag"
fi
lock
wait_db

# For a schema change, pull the release's image before anything stops.
if [[ -n ${revision:-} ]]; then
    newest_release >/dev/null
    if is_pre_release && pre_release_image_kept; then
        revision=$pre_release
        image="$IMAGE:$PRE_RELEASE_TAG"
    else
        revision=$(git -C "$REPO" rev-parse --verify "$revision^{commit}") || fail "no revision $revision in the clone"
        tag=$(release_of "$revision")
        if [[ -z $tag ]]; then
            hint=''
            if pre_release_image_kept; then hint=", or $pre_release"; fi
            fail "$revision is not a release, so it has no image; pass a release tag$hint"
        fi
        if ! run podman image exists "$IMAGE:$revision"; then
            pull_release "$tag" amd64 "$IMAGE:$revision" || fail "could not pull the image for $tag"
        fi
        image="$IMAGE:$revision"
    fi
fi
previous_image=$(run podman image inspect --format '{{.Id}}' "$IMAGE:current")

# Lowercase, because Postgres folds unquoted names to lowercase.
stamp=$(date -u +%Y%m%dt%H%M%Sz)
target="skyblock_restore_$stamp"
psql_q -d postgres -c "CREATE DATABASE $target"
# Until the swap, a failure leaves the live database untouched and drops the new copy.
drop_partial() {
    psql_q -d postgres -c "DROP DATABASE IF EXISTS $target WITH (FORCE)" >/dev/null ||
        log "could not drop $target; drop it by hand"
}
trap drop_partial EXIT

if [[ -n $point ]]; then
    (cd "$point" && sha256sum --quiet -c SHA256SUMS) || fail 'restore point failed its checksums'
    log "restoring $point into $target"
    restore() { db_exec pg_restore -h /var/run/postgresql -U collector -d "$target" --exit-on-error --no-owner "$@"; }
    restore --section=pre-data <"$point/database.dump"
    while IFS='|' read -r name _ <&3; do
        [[ -n $name ]] || continue
        restore --data-only --disable-triggers <"$point/partitions/$name.dump"
    done 3<"$point/partitions.txt"
    restore --section=data --disable-triggers <"$point/database.dump"
    restore --section=post-data <"$point/database.dump"
    expected=$(wc -l <"$point/partitions.txt")
    actual=$(DB_NAME=$target psql_q -c "SELECT count(*) FROM partition_days WHERE closed_at IS NOT NULL")
    ((actual == expected)) || fail "restored $actual closed partitions; the restore point lists $expected"
    DB_NAME=$target psql_q -c "SELECT count(*) FROM sales" >/dev/null
    # A restore point taken before a schema change holds the old schema, which the collector
    # refuses. Stop here, while the live database is still untouched.
    schema_of() { DB_NAME=$1 psql_q -c "SELECT encode(checksum, 'hex') FROM _sqlx_migrations WHERE version = 1"; }
    [[ $(schema_of "$target") == "$(schema_of skyblock)" ]] ||
        fail "the restore point has a different schema than the live database; restore a newer point or use --rebuild"
    # Archive files from the day before the restore point onward. Rows already present are
    # skipped; rows for closed days are refused by their partitions and were already restored.
    since=$(date -u -d "@$(($(stat -c %Y "$point/database.dump") - 86400))" +%F)
else
    log "rebuilding into $target from the archive alone"
    since=0000-00-00
fi

# Replay into the new database through its own connection settings, with the image that matches
# its schema. Until the stop, a failure also makes sure the collector runs.
target_env=$(mktemp -p /tmp replay-env.XXXXXX)
replayed=$(mktemp)
before_stop() {
    rm -f -- "$target_env" "$replayed"
    drop_partial
    run systemctl --user start "$APP_SERVICE" || true
}
trap before_stop EXIT
password=$(db_password)
{ grep -v '^DATABASE_URL=' "$APP/collector.env" || true; } >"$target_env"
printf 'DATABASE_URL=postgres://collector:%s@localhost/%s?host=/run/postgresql\n' "$password" "$target" >>"$target_env"
replay_new() {
    local files
    mapfile -t files < <(find "$APP/data/archive" -type f -name '*.ndjson.zst' | while IFS= read -r f; do
        day=$(basename "$(dirname "$f")")
        if [[ ! $day < $since ]]; then
            printf '/data/archive/%s\n' "${f#"$APP/data/archive/"}"
        fi
    done | LC_ALL=C sort | LC_ALL=C comm -23 - "$replayed")
    log "replaying ${#files[@]} archive files into $target"
    if ((${#files[@]} > 0)); then
        COLLECTOR_IMAGE=${image:-$IMAGE:current} COLLECTOR_ENV_FILE=$target_env RUN_TIMEOUT=48h \
            collector_cmd replay "${files[@]}"
        printf '%s\n' "${files[@]}" | LC_ALL=C sort -m - "$replayed" -o "$replayed"
    fi
}

# The passes need the collector collecting, not the current database writable: a clean stop
# finishes the archive files either way.
wait_running() {
    local i
    for ((i = 0; i < 60; i++)); do
        run podman healthcheck run "$APP_CONTAINER" >/dev/null 2>&1 && return 0
        sleep 5
    done
    return 1
}

# Each pass replays the files completed since the previous one. Restarting the collector completes
# the files it has open, at the cost of a few seconds; the ended-auctions feed covers the last
# minute, so that loses nothing. Passes shrink until one takes under a minute, which bounds the
# replay done while the collector is stopped.
pass_seconds=0
for _ in 1 2 3 4 5; do
    run systemctl --user restart "$APP_SERVICE"
    wait_running || fail 'the collector is not running after a restart'
    # Right after a restart the collector may still be connecting.
    for ((i = 0; i < 6; i++)); do
        curl -fsS -m 5 "$STATUS_URL" | jq -e '.ingest.database_ok' >/dev/null 2>&1 && break
        if ((i < 5)); then sleep 5; fi
    done
    ((i < 6)) || log 'warning: the current database is not accepting writes; collected data waits in the spool'
    started=$SECONDS
    replay_new
    pass_seconds=$((SECONDS - started))
    ((pass_seconds < 60)) && break
done
((pass_seconds < 60)) || fail "the last replay pass took $pass_seconds s; the collector was not stopped. Run restore.sh again"

# The secondary collector's files hold snapshots the archive lacks. A pull skips while this holds
# the lock, so none arrive until the swap.
mapfile -t secondary < <(find "$SECONDARY_DATA/archive" "$SECONDARY_DATA/inbox" -type f -name '*.ndjson.zst' 2>/dev/null |
    while IFS= read -r f; do
        day=$(basename "$(dirname "$f")")
        [[ $day < $since ]] || printf '/data/secondary/%s\n' "${f#"$SECONDARY_DATA/"}"
    done | LC_ALL=C sort)
if ((${#secondary[@]} > 0)); then
    log "backfilling from ${#secondary[@]} secondary files into $target"
    COLLECTOR_IMAGE=${image:-$IMAGE:current} COLLECTOR_ENV_FILE=$target_env RUN_TIMEOUT=48h \
        collector_cmd backfill "${secondary[@]}"
fi

log 'stopping the collector for the last files and the swap'
trap - EXIT
# Every stopped minute is a lost minute of sales. Whatever fails from here, it starts again, and
# until the swap begins the new database is dropped.
swapping=''
after_stop() {
    local named
    rm -f -- "$target_env" "$replayed"
    [[ -n $swapping ]] || drop_partial
    if ! named=$(psql_q -d postgres -c "SELECT count(*) FROM pg_database WHERE datname = 'skyblock'" 2>/dev/null); then
        log "could not reach Postgres to check the database swap; check that a database is named skyblock"
    elif [[ $named != 1 ]]; then
        log "the database swap did not finish: no database is named skyblock; rename skyblock_before_restore_$stamp or $target to skyblock (the collector spools until then)"
    elif [[ -n $swapping && $(psql_q -d postgres -c "SELECT count(*) FROM pg_database WHERE datname = '$target'" 2>/dev/null) == 1 ]]; then
        log "the swap failed before it renamed anything; the collector keeps the current database. $target is kept; drop it with: podman exec -e PGPASSWORD=\"\$(cat $APP/secrets/db-password)\" $DB_CONTAINER psql -h /var/run/postgresql -U collector -d postgres -c 'DROP DATABASE $target'"
    fi
    run systemctl --user start "$APP_SERVICE" || true
}
trap after_stop EXIT
stopped_at=$SECONDS
run systemctl --user stop "$APP_SERVICE"
# A stop that timed out leaves today's open archive files unfinished; finish them so they replay.
ARCHIVE_MODE=rw collector_cmd recover-archive
replay_new
swapping=1
psql_q -d postgres -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname IN ('skyblock', '$target') AND pid <> pg_backend_pid()" >/dev/null
psql_q -d postgres -c "ALTER DATABASE skyblock RENAME TO skyblock_before_restore_$stamp"
psql_q -d postgres -c "ALTER DATABASE $target RENAME TO skyblock"
if [[ -n $image ]]; then
    run podman tag "$image" "$IMAGE:current" ||
        fail "could not tag $image as current; the old image starts on the swapped-in database"
fi
run systemctl --user start "$APP_SERVICE"
trap - EXIT
rm -f -- "$target_env" "$replayed"
log "the collector was stopped for $((SECONDS - stopped_at)) s"
wait_collector || fail "the collector is not healthy after the swap. To go back: stop it, rename skyblock to $target and skyblock_before_restore_$stamp to skyblock, run podman tag $previous_image $IMAGE:current, and start it again"
# A deploy of this revision that failed before the rebuild (against the old schema) can go
# through now; update.sh installs its deploy files and records it.
if [[ -n ${revision:-} && $(cat "$APP/failed-rev" 2>/dev/null || true) == "$revision" ]]; then
    rm -f -- "$APP/failed-rev"
fi
log "done; the previous database is kept as skyblock_before_restore_$stamp"
printf 'Drop the previous database when you are satisfied:\n  podman exec -e PGPASSWORD="%s" %s psql -h /var/run/postgresql -U collector -d postgres -c "DROP DATABASE skyblock_before_restore_%s"\n' "\$(cat $APP/secrets/db-password)" "$DB_CONTAINER" "$stamp"
