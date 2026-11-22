#!/usr/bin/env bash
# Makes a restore point and checks the raw archive. Usage: backup.sh [daily|manual]
#
# 1. Closes the daily partitions of finished UTC days, once nothing waiting in the spool or still
#    coming from the secondary collector belongs to them. A closed partition refuses writes.
# 2. Dumps each newly closed partition once, into backups/partitions/.
# 3. Dumps everything else (all schema, the small tables, and open partitions) into a new
#    restore point, which hard-links the partition dumps, so each day's data is stored once.
# 4. Records checksums for new archive files and re-checks every recorded checksum.
# 5. Keeps the newest daily restore point from each of the last 7 days and the 3 newest manual ones.
# shellcheck source=common.sh source-path=SCRIPTDIR
source "$(dirname "$(realpath "$0")")/common.sh"

KIND=${1:-manual}
[[ $KIND =~ ^(daily|manual)$ ]] || fail "unknown backup kind: $KIND"
MIN_FREE_GB=${MIN_FREE_GB:-40}
SPOOL="$APP/data/spool"
ARCHIVE="$APP/data/archive"
MANIFEST="$BACKUPS/archive.sha256"

[[ ${SKC_LOCK_HELD:-} == 1 ]] || lock
mkdir -p "$POOL"
free_gb=$(($(df --output=avail -k "$BACKUPS" | tail -1) / 1024 / 1024))
((free_gb >= MIN_FREE_GB)) || fail "only ${free_gb} GB free on the backup disk; need ${MIN_FREE_GB} GB"
wait_db

WORK=$(mktemp -d "$BACKUPS/.work-XXXXXXXX")
cleanup() { rm -rf -- "$WORK"; }
trap cleanup EXIT

# 1. Close finished days. The oldest spooled job decides: a day stays open while a job from it
# (or from before it) may still be written.
close_before=$(date -u +%F)
# sed reads its whole input; head would exit early and fail the pipeline with SIGPIPE.
oldest_job=$(find "$SPOOL" -maxdepth 1 -name '*.job' -printf '%f\n' 2>/dev/null | sort | sed -n 1p)
if [[ -n $oldest_job ]]; then
    # A job's rows can be older than its file: a sales snapshot covers the 60 s before it, and a
    # cached auction page can be 5 minutes stale. 15 minutes covers both.
    oldest_day=$(date -u -d "@$((10#${oldest_job:0:20} / 1000000000 - 900))" +%F)
    if [[ $oldest_day < $close_before ]]; then
        close_before=$oldest_day
    fi
fi
# A day also stays open until the secondary collector's files from it are backfilled: each feed's
# newest backfilled file must be from a later day, and no file from it may wait in the inbox. A
# secondary that stops sending holds days open for at most 3 days.
secondary_before=$(date -u +%F)
while IFS= read -r day; do
    if [[ $day < $secondary_before ]]; then secondary_before=$day; fi
done < <(
    find "$SECONDARY_DATA/inbox" -mindepth 2 -maxdepth 2 -type d -printf '%f\n' 2>/dev/null
    for feed_dir in "$SECONDARY_DATA"/archive/*/; do
        if [[ -d $feed_dir ]]; then
            find "$feed_dir" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' | LC_ALL=C sort | sed -n '$p'
        fi
    done
)
floor=$(date -u -d "$(date -u +%F) - 3 days" +%F)
if [[ $secondary_before < $floor ]]; then secondary_before=$floor; fi
if [[ $secondary_before < $close_before ]]; then close_before=$secondary_before; fi
# Keep the most recent finished day open for 30 minutes after midnight UTC so late writes land.
if (($(date -u +%s) - $(date -u -d "$(date -u +%F)" +%s) < 1800)); then
    close_before=$(date -u -d "$close_before - 1 day" +%F)
fi
# Closing renames partitions, which waits for open transactions on them and blocks the collector's
# writes meanwhile. Give up after 30 s and leave them open; this restore point then dumps those
# days with the open partitions instead.
if closed=$(psql_q -q -c "SET lock_timeout = '30s'" -c "SELECT count(*) FROM close_partitions('$close_before')" 2>"$WORK/close.err"); then
    log "closed $closed partitions before $close_before"
else
    log "could not close partitions before $close_before ($(tr '\n' ' ' <"$WORK/close.err")); they stay open until the next backup"
fi

# 2. Dump closed partitions that have no dump for their current closing.
dumped=0
while IFS='|' read -r name closed_epoch <&3; do
    [[ -n $name ]] || continue
    file="$POOL/$name-$closed_epoch.dump"
    [[ -s $file && -s $file.sha256 ]] && continue
    db_exec pg_dump -h /var/run/postgresql -U collector -d skyblock -Fc -Z zstd:9 --data-only \
        --table="public.$name" >"$WORK/part.dump"
    db_exec pg_restore --list <"$WORK/part.dump" >/dev/null
    sha=$(sha256sum "$WORK/part.dump" | cut -d' ' -f1)
    sync -f "$WORK/part.dump"
    mv -- "$WORK/part.dump" "$file"
    printf '%s  %s\n' "$sha" "${file##*/}" >"$file.sha256"
    chmod 400 "$file" "$file.sha256"
    dumped=$((dumped + 1))
done 3< <(psql_q -F '|' -c "SELECT partition_name, extract(epoch FROM closed_at)::bigint FROM partition_days WHERE closed_at IS NOT NULL ORDER BY day, parent")
log "dumped $dumped newly closed partitions"

# 3. The restore point.
stamp="$(date +%F)-$(date -u +%Y%m%dT%H%M%SZ)"
point="$BACKUPS/$KIND-$stamp"
stage="$WORK/point"
mkdir -p "$stage/partitions" "$stage/config"
db_exec pg_dump -h /var/run/postgresql -U collector -d skyblock -Fc -Z zstd:6 \
    --exclude-table-data='public.*_closed_*' >"$stage/database.dump"
db_exec pg_restore --list <"$stage/database.dump" >/dev/null
psql_q -F '|' -c "SELECT partition_name, extract(epoch FROM closed_at)::bigint FROM partition_days WHERE closed_at IS NOT NULL ORDER BY day, parent" >"$stage/partitions.txt"
while IFS='|' read -r name closed_epoch <&3; do
    [[ -n $name ]] || continue
    file="$POOL/$name-$closed_epoch.dump"
    [[ -s $file ]] || fail "closed partition $name has no dump"
    (cd "$POOL" && sha256sum --quiet -c "${file##*/}.sha256") || fail "dump of $name failed its checksum"
    ln -- "$file" "$stage/partitions/$name.dump"
done 3<"$stage/partitions.txt"
cp -a -- "$APP/collector.env" "$APP/secrets" "$APP/quadlet" "$APP/systemd" "$APP/postgres" "$stage/config/"
if [[ -f $APP/deploy.env ]]; then cp -a -- "$APP/deploy.env" "$stage/config/"; fi
cp -- "$APP"/*.sh "$stage/config/"
[[ -f $APP/installed-rev ]] && cp -- "$APP/installed-rev" "$stage/config/"
(cd "$stage" && find . -type f -print0 | sort -z | xargs -0 sha256sum >"$WORK/SHA256SUMS")
mv -- "$WORK/SHA256SUMS" "$stage/SHA256SUMS"
sync -f "$stage/database.dump"
mv -- "$stage" "$point"
sync -f "$BACKUPS"
log "restore point published: $point ($(du -sh --apparent-size "$point/database.dump" | cut -f1) database dump, $(wc -l <"$point/partitions.txt") closed partitions)"
printf '%s\n' "$point" >"$WORK/last-point"
LAST_POINT=$point

# 4. Archive checks: record new complete files, then verify every recorded checksum.
touch "$MANIFEST"
new_files=0
while IFS= read -r -d '' file; do
    rel=${file#"$ARCHIVE/"}
    grep -qF "  $rel" "$MANIFEST" && continue
    zstd -tq --long=30 -- "$file" || fail "archive file fails its zstd check: $rel"
    (cd "$ARCHIVE" && sha256sum -- "$rel") >>"$MANIFEST"
    new_files=$((new_files + 1))
done < <(find "$ARCHIVE" -type f -name '*.ndjson.zst' -print0 | sort -z)
(cd "$ARCHIVE" && sha256sum --quiet -c "$MANIFEST") || fail 'an archive file no longer matches its recorded checksum'
log "archive: $new_files new files recorded; $(wc -l <"$MANIFEST") files verified"

# 5. Retention.
previous_day='' count=0
while IFS= read -r name; do
    day=${name:6:10}
    if [[ $day == "$previous_day" ]] || ((count >= 7)); then
        rm -rf -- "${BACKUPS:?}/$name"
        continue
    fi
    previous_day=$day
    count=$((count + 1))
done < <(find "$BACKUPS" -maxdepth 1 -type d -name 'daily-*' -printf '%f\n' | sort -r)
find "$BACKUPS" -maxdepth 1 -type d -name 'manual-*' -printf '%f\n' | sort -r | tail -n +4 |
    while IFS= read -r name; do rm -rf -- "${BACKUPS:?}/$name"; done
# A partition dump that no restore point links, and that a newer dump of the same partition
# replaced, is no longer needed.
while IFS= read -r -d '' file; do
    base=${file##*/}
    partition=${base%-*}
    newest=$(find "$POOL" -maxdepth 1 -name "$partition-*.dump" -printf '%f\n' | sort | tail -1)
    if [[ $base != "$newest" && $(stat -c %h "$file") == 1 ]]; then
        rm -f -- "$file" "$file.sha256"
    fi
done < <(find "$POOL" -maxdepth 1 -name '*.dump' -print0)
log "backup done: $LAST_POINT"
