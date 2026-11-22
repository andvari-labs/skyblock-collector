#!/usr/bin/env bash
# Sets up or repairs the deployment in ~/services/skyblock-collector from the checkout this script
# is in, which must be a release: it runs that release's image. Safe to run again: it keeps
# existing data, secrets, and settings. It reads the host's settings from deploy.env in the
# deployment's folder (see deploy.env.example).
#
# When it replaces a collector that runs, as at the move from building on this host to released
# images, it keeps the files and image that ran, and puts them back if the new release isn't
# healthy within five minutes.
# shellcheck source=common.sh source-path=SCRIPTDIR
source "$(dirname "$(realpath "$0")")/common.sh"

if [[ -n ${SKC_DEPLOY_ENV:-} && $NAME == skyblock-collector ]]; then
    fail "$SKC_DEPLOY_ENV sets no NAME of its own, so it would replace the deployment's deploy.env; give the test install its own NAME"
fi
SRC=$(realpath "$KIT_DIR/../../..")
[[ -f $SRC/Containerfile && -d $SRC/$KIT_PATH ]] || fail "run install.sh from a skyblock-collector checkout, not $SRC"
require_credential
lock
git_env
log "settings: status server on $BIND_ADDRESS:$STATUS_PORT, Postgres on $BIND_ADDRESS:$PG_PORT, secondary collector on ${SECONDARY_HOST:-none}, image $REGISTRY_IMAGE, credential ${REGISTRY_AUTHFILE:-none}"

mkdir -p "$APP"/{data/postgres,data/archive,data/spool,data/socket,data/secondary,secrets,logs,backups/partitions}
chmod 700 "$APP" "$APP/secrets" "$APP/backups"
# Postgres creates its socket here as uid 999, which keep-id maps to the host user.
chmod 755 "$APP/data/socket"
if [[ -n ${SKC_DEPLOY_ENV:-} && ! $SKC_DEPLOY_ENV -ef $APP/deploy.env ]]; then
    install -m 600 -- "$SKC_DEPLOY_ENV" "$APP/deploy.env"
elif [[ ! -e $APP/deploy.env ]]; then
    # Every later script refuses to run a deployment without the file, so a first install with
    # the defaults writes an empty one.
    install -m 600 /dev/null "$APP/deploy.env"
fi

if [[ ! -d $REPO/.git ]]; then
    log "cloning $REMOTE into $REPO"
    run git clone --quiet "$REMOTE" "$REPO"
fi
revision=$(git -C "$SRC" rev-parse HEAD)
newest_release >/dev/null
git -C "$REPO" cat-file -e "$revision^{commit}" 2>/dev/null || fail "commit $revision is not on GitHub yet; push it first"
tag=$(release_of "$revision")
[[ -n $tag ]] || fail "commit $revision is not a release; check out a release tag (git checkout v<version>) and run install.sh from there"
if ! run podman image exists "$IMAGE:$revision"; then
    pull_release "$tag" amd64 "$IMAGE:$revision" || fail "could not pull the image for $tag"
fi
installed=$(cat "$APP/installed-rev" 2>/dev/null || true)
run git -C "$REPO" checkout --quiet --detach "$revision"

new_secret() {
    [[ -s $1 ]] && return 0
    head -c 32 /dev/urandom | sha256sum | cut -c1-48 >"$1"
    chmod 600 "$1"
    log "created ${1##*/}"
}
new_secret "$APP/secrets/db-password"
new_secret "$APP/secrets/reader-password"
if [[ ! -s $APP/collector.env ]]; then
    cat >"$APP/collector.env" <<ENV
DATABASE_URL=postgres://collector:$(db_password)@localhost/skyblock?host=/run/postgresql
RUST_LOG=info
ENV
    chmod 600 "$APP/collector.env"
fi

keep_pre_release_image
# What runs now, to put back if the new release isn't healthy.
saved=$(mktemp -d)
trap 'rm -rf -- "$saved"' EXIT
previous_image=$(run podman image inspect --format '{{.Id}}' "$IMAGE:current" 2>/dev/null || true)
if [[ -n $previous_image ]]; then
    for d in quadlet systemd postgres; do
        if [[ -e $APP/$d ]]; then cp -a -- "$APP/$d" "$saved/"; fi
    done
    for f in "${SCRIPTS[@]}"; do
        if [[ -e $APP/$f ]]; then cp -a -- "$APP/$f" "$saved/"; fi
    done
fi

# Installs the release's files and image and starts it. Every step's failure returns 1, so what
# ran before goes back whichever step fails, not only the health check.
pod_restarted=0
start_release() {
    local image_for_db ports_before
    ports_before=$(published_ports "$APP/quadlet/$NAME.pod")
    install_deploy_files "$REPO/$KIT_PATH" || return 1
    link_units || return 1
    image_for_db=$(sed -n 's/^Image=//p' "$APP/quadlet/$NAME-db.container") || return 1
    run podman image exists "$image_for_db" || run podman pull --quiet "$image_for_db" >/dev/null || return 1
    run podman tag "$IMAGE:$revision" "$IMAGE:current" || return 1
    if [[ -n $ports_before && $(published_ports "$APP/quadlet/$NAME.pod") != "$ports_before" ]] &&
        systemctl --user is-active --quiet "$POD_SERVICE"; then
        pod_restarted=1
        restart_pod 'the published address or ports changed' || return 1
    else
        run systemctl --user start "$POD_SERVICE" "$DB_SERVICE" || return 1
        (wait_db) || return 1
    fi
    run systemctl --user restart "$APP_SERVICE" || return 1
    wait_collector
}
if ! start_release; then
    [[ -n $previous_image ]] || fail 'the collector did not become healthy'
    log "$tag ($revision) did not start healthy; putting back what ran before"
    printf '%s\n' "$revision" >"$APP/failed-rev"
    for d in quadlet systemd postgres; do
        if [[ -e $saved/$d ]]; then rm -rf -- "${APP:?}/$d" && cp -a -- "$saved/$d" "$APP/"; fi
    done
    for f in "${SCRIPTS[@]}"; do
        if [[ -e $saved/$f ]]; then cp -- "$saved/$f" "$APP/$f.new.$$" && mv -f -- "$APP/$f.new.$$" "$APP/$f"; fi
    done
    link_units
    run podman tag "$previous_image" "$IMAGE:current"
    if ((pod_restarted)); then restart_pod 'putting back the published address and ports that ran before' || true; fi
    run systemctl --user restart "$APP_SERVICE"
    if [[ -n $installed ]]; then
        run git -C "$REPO" checkout --quiet --detach "$installed" 2>/dev/null || true
    fi
    wait_collector || fail "what ran before is unhealthy too; manual recovery needed (see README.md)"
    fail "put back what ran before; $tag ($revision) is marked failed and the timers are left as they were"
fi

psql_q -c "ALTER ROLE reader LOGIN PASSWORD '$(cat "$APP/secrets/reader-password")'" >/dev/null
# A reader session left idle inside a transaction (a notebook's default) holds locks that block
# creating and closing partitions. End such sessions after 10 minutes.
psql_q -c "ALTER ROLE reader SET idle_in_transaction_session_timeout = '10min'" >/dev/null

if [[ $ENABLE_TIMERS == 1 ]]; then run systemctl --user enable --now "${TIMERS[@]}"; fi
printf '%s\n' "$revision" >"$APP/installed-rev"
rm -f -- "$APP/failed-rev"
log "installed $tag ($revision)"
if [[ -z $SECONDARY_HOST ]]; then
    run systemctl --user disable --now "$NAME-pull.timer" 2>/dev/null || true
    log "deploy.env names no SECONDARY_HOST, so no secondary collector runs"
elif ! "$APP/secondary.sh" "$revision" "$REPO"; then
    date +%s >"$APP/secondary-failed"
    log "the secondary collector did not deploy; update.sh retries it within the hour"
fi
