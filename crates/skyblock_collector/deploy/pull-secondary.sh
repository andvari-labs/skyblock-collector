#!/usr/bin/env bash
# Pulls the secondary collector's finished archive files, then writes the snapshots the database
# lacks from them. skyblock-collector-pull.timer runs it every 5 minutes.
#
# A file is deleted on $SECONDARY_HOST only once it is copied here, so while this host is down
# or offline the secondary keeps everything. Pulled files wait in $SECONDARY_DATA/inbox until the
# backfill has written them, then move to $SECONDARY_DATA/archive, which is never deleted.
# shellcheck source=common.sh source-path=SCRIPTDIR
source "$(dirname "$(realpath "$0")")/common.sh"
[[ -n $SECONDARY_HOST ]] || fail 'deploy.env names no SECONDARY_HOST, so there is nothing to pull'

mkdir -p "$BACKUPS"
exec 9>"$BACKUPS/.lock"
if ! flock -n 9; then
    log 'skipped: another backup, update, restore, or pull holds the lock'
    exit 0
fi
inbox="$SECONDARY_DATA/inbox"
kept="$SECONDARY_DATA/archive"
mkdir -p "$inbox" "$kept"

# rsync removes each source file only after it is written and synced here. Open hours are skipped.
if RUN_TIMEOUT=30m run rsync -a --fsync --remove-source-files --exclude='*.partial' \
    -e 'ssh -o BatchMode=yes -o ConnectTimeout=20' \
    "$SECONDARY_HOST:$SECONDARY_APP/data/archive/" "$inbox/"; then
    # Day directories left empty, except today's, which the collector may be about to write in.
    secondary_ssh find "$SECONDARY_APP/data/archive" -mindepth 2 -maxdepth 2 -type d -empty \
        ! -name "$(date -u +%F)" -delete || true
else
    log "could not pull from $SECONDARY_HOST (exit $?); backfilling what is already here"
fi

mapfile -t files < <(cd "$inbox" && find . -type f -name '*.ndjson.zst' | sed 's|^\./||' | LC_ALL=C sort)
((${#files[@]})) || exit 0
args=()
for f in "${files[@]}"; do args+=("/data/secondary/inbox/$f"); done
RUN_TIMEOUT=6h collector_cmd backfill "${args[@]}" ||
    fail "the backfill of ${#files[@]} files failed; they stay in $inbox for the next run"
for f in "${files[@]}"; do
    mkdir -p "$kept/$(dirname "$f")"
    mv -- "$inbox/$f" "$kept/$f"
done
find "$inbox" -mindepth 1 -type d -empty -delete
log "backfilled from ${#files[@]} secondary files"
