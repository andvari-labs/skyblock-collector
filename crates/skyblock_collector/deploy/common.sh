#!/usr/bin/env bash
# Shared by the deployment's scripts. Sourced, not run.
set -Eeuo pipefail
umask 077

# The host's own settings: the deployment reads deploy.env in its folder
# (~/services/skyblock-collector/deploy.env), which is never in the repository; deploy.env.example
# lists every setting. Each one deploy.env leaves out keeps its default below. Only that file
# overrides them; the environment doesn't, so a variable such as NAME exported in a shell can't
# redirect the scripts. The kit in a checkout takes a test install's own file from SKC_DEPLOY_ENV,
# and install.sh copies that file into the install; installed scripts read only their own.
NAME=skyblock-collector
BIND_ADDRESS=127.0.0.1
STATUS_PORT=3004
PG_PORT=5432
# shellcheck disable=SC2034 # read by the scripts that source this file
ENABLE_TIMERS=1
# shellcheck disable=SC2034 # read by the scripts that source this file
REMOTE=https://github.com/andvari-labs/skyblock-collector.git
REGISTRY_IMAGE=ghcr.io/andvari-labs/skyblock-collector
REGISTRY_AUTHFILE=
SECONDARY_HOST=
# shellcheck disable=SC2034 # read by the scripts that source this file
SECONDARY_ARCH=arm64
KIT_DIR=$(dirname "$(realpath "${BASH_SOURCE[0]}")")
if [[ ${KIT_DIR##*/} != deploy ]]; then
    DEPLOY_ENV=$KIT_DIR/deploy.env
elif [[ -n ${SKC_DEPLOY_ENV:-} ]]; then
    DEPLOY_ENV=$SKC_DEPLOY_ENV
    if [[ ! -f $DEPLOY_ENV || ! -r $DEPLOY_ENV ]]; then
        printf 'SKC_DEPLOY_ENV names %s, which is not a readable file; refusing to fall back to the deployment\n' "$DEPLOY_ENV" >&2
        exit 1
    fi
else
    # The kit in a checkout reads the deployment's file, never one lying in the checkout.
    DEPLOY_ENV=$HOME/services/$NAME/deploy.env
fi
if [[ -f $DEPLOY_ENV ]]; then
    # shellcheck disable=SC1090 # deploy.env is the host's own file, outside the kit
    source "$DEPLOY_ENV"
fi
# Where the kit sits in a checkout of the repository.
# shellcheck disable=SC2034 # read by the scripts that source this file
KIT_PATH=crates/skyblock_collector/deploy

APP="$HOME/services/$NAME"
# A deployment that already runs keeps its settings in deploy.env; without the file every setting
# would quietly fall back to its default, such as publishing on 127.0.0.1 only.
if [[ -e $APP/installed-rev && ! -f $DEPLOY_ENV ]]; then
    printf '%s is missing, but %s holds a deployment; write it from deploy.env.example (an empty file keeps every default)\n' "$DEPLOY_ENV" "$APP" >&2
    exit 1
fi
REPO="$APP/repo"
BACKUPS="$APP/backups"
# shellcheck disable=SC2034 # read by the scripts that source this file
POOL="$BACKUPS/partitions"
DB_CONTAINER=$NAME-db
APP_CONTAINER=$NAME
# shellcheck disable=SC2034 # read by the scripts that source this file
APP_SERVICE=$NAME.service
# shellcheck disable=SC2034 # read by the scripts that source this file
DB_SERVICE=$NAME-db.service
# shellcheck disable=SC2034 # read by the scripts that source this file
POD_SERVICE=$NAME-pod.service
IMAGE=localhost/$NAME
STATUS_URL=http://$BIND_ADDRESS:$STATUS_PORT/status
# Each release publishes one image for x86-64 and ARM64. A public image needs no credential; for a
# private one, REGISTRY_AUTHFILE in deploy.env names a podman auth file with read access to it,
# which the pulls read in place.
# The image a restore can rebuild a revision from before the first release: install.sh tags the
# image that ran before it as :pre-trellis and records that revision in pre-trellis-rev.
PRE_RELEASE_TAG=pre-trellis
# How long update.sh waits for a release's image before it marks the release failed.
# shellcheck disable=SC2034 # read by the scripts that source this file
MISSING_IMAGE_LIMIT_SECS=$((6 * 3600))
# The secondary collector runs on another network, so an outage here does not lose data. Its
# paths are relative to its user's home there.
# shellcheck disable=SC2034 # read by the scripts that source this file
SECONDARY_APP=services/$NAME-secondary
# shellcheck disable=SC2034 # read by the scripts that source this file
SECONDARY_SERVICE=$NAME-secondary.service
# shellcheck disable=SC2034 # read by the scripts that source this file
SECONDARY_CONTAINER=$NAME-secondary
# shellcheck disable=SC2034 # read by the scripts that source this file
SECONDARY_IMAGE=localhost/$NAME-secondary
# shellcheck disable=SC2034 # read by the scripts that source this file
SECONDARY_UNIT=.config/containers/systemd/$NAME-secondary.container
SECONDARY_DATA="$APP/data/secondary"
# Units and files install.sh and update.sh copy from the kit into $APP, named for the deployment.
QUADLETS=(skyblock-collector.pod skyblock-collector-db.container skyblock-collector.container)
UNITS=(skyblock-collector-backup.service skyblock-collector-backup.timer skyblock-collector-update.service skyblock-collector-update.timer skyblock-collector-pull.service skyblock-collector-pull.timer)
# shellcheck disable=SC2034 # read by the scripts that source this file
TIMERS=("$NAME-backup.timer" "$NAME-update.timer")
# The pull timer copies the secondary collector's files, so only a deployment with one runs it.
if [[ -n $SECONDARY_HOST ]]; then TIMERS+=("$NAME-pull.timer"); fi
SCRIPTS=(common.sh install.sh backup.sh restore.sh update.sh verify.sh secondary.sh pull-secondary.sh)

log() {
    mkdir -p "$APP/logs"
    printf '%s %s: %s\n' "$(date --iso-8601=seconds)" "${0##*/}" "$*" | tee -a "$APP/logs/operations.log" >&2
}
fail() {
    log "FAILED: $*"
    exit 1
}
on_error() {
    # Name the script's line, also when the command that failed ran inside a helper here such as run.
    local i
    for ((i = 1; i < ${#BASH_SOURCE[@]} - 1; i++)); do
        [[ ${BASH_SOURCE[i]} == "${BASH_SOURCE[0]}" ]] || break
    done
    log "FAILED: ${BASH_SOURCE[i]##*/} line ${BASH_LINENO[i - 1]} (exit $1)"
}
trap 'on_error "$?"' ERR

# Bounds every external command so a hung step cannot hold the lock forever. The command does not
# inherit the lock (fd 9), so a process it leaves behind cannot hold the lock either.
run() { timeout --foreground --kill-after=30s "${RUN_TIMEOUT:-2h}" "$@" 9>&-; }

git_env() {
    export GIT_TERMINAL_PROMPT=0 GIT_ASKPASS='' SSH_ASKPASS='' GCM_INTERACTIVE=false
    GIT_SSH_COMMAND="${GIT_SSH_COMMAND:-$(git -C "$REPO" config --get core.sshCommand 2>/dev/null || echo ssh)} -o BatchMode=yes"
    export GIT_SSH_COMMAND
}

# One lock for every operation that stops services, dumps the database, or writes to it in bulk.
lock() {
    mkdir -p "$BACKUPS"
    exec 9>"$BACKUPS/.lock"
    flock -w "${LOCK_WAIT:-3600}" 9 || fail 'another backup, update, restore, or pull holds the lock'
}

# Runs a command on the secondary host.
secondary_ssh() { run ssh -o BatchMode=yes -o ConnectTimeout=20 "$SECONDARY_HOST" "$@"; }

db_password() { cat "$APP/secrets/db-password"; }

# psql and pg_dump run inside the database container over its Unix socket.
db_exec() {
    run podman exec -i -e PGPASSWORD="$(db_password)" "$DB_CONTAINER" "$@"
}
psql_q() {
    db_exec psql -X -v ON_ERROR_STOP=1 -h /var/run/postgresql -U collector -d "${DB_NAME:-skyblock}" -At "$@"
}

wait_db() {
    local i
    for ((i = 0; i < 120; i++)); do
        if run podman exec "$DB_CONTAINER" pg_isready -q -h /var/run/postgresql -U collector -d skyblock 2>/dev/null; then
            return 0
        fi
        sleep 5
    done
    fail 'Postgres did not become ready within 10 minutes'
}

# Waits until the collector container is healthy and reports a working database.
wait_collector() {
    local i
    for ((i = 0; i < 60; i++)); do
        if run podman healthcheck run "$APP_CONTAINER" >/dev/null 2>&1 &&
            curl -fsS -m 5 "$STATUS_URL" | jq -e '.ingest.database_ok == true' >/dev/null 2>&1; then
            return 0
        fi
        sleep 5
    done
    return 1
}

# The newest release tag in the clone, after fetching the tags from GitHub, leaving out any
# pre-release such as v1.0.0-rc.1. Prints nothing when there is no release yet.
newest_release() {
    git_env
    run git -C "$REPO" fetch --quiet --force --tags origin || fail "could not fetch the release tags from GitHub (exit $?)"
    git -C "$REPO" tag --list 'v[0-9]*' --sort=-v:refname | awk '!/-/ && !n++'
}

# The release tag that points at a revision, or nothing when it was never released.
release_of() {
    git -C "$REPO" tag --list 'v[0-9]*' --points-at "$1" --sort=-v:refname | awk '!/-/ && !n++'
}

# Pulls the image of a release for one processor (amd64 or arm64) and tags it as $3. A refused
# pull stops the script with what to do; nothing running is touched before this. Without
# REGISTRY_AUTHFILE it pulls with an empty auth file, so podman can't fall back to a credential in
# its default auth file that another service keeps there. Returns 1 when the image can't be pulled
# for another reason, such as not being pushed yet.
pull_release() {
    local tag=$1 arch=$2 name=$3 id tmp err authfile
    require_credential
    tmp=$(mktemp -d)
    err=$tmp/err
    authfile=${REGISTRY_AUTHFILE:-$tmp/auth.json}
    if [[ -z $REGISTRY_AUTHFILE ]]; then printf '{"auths":{}}\n' >"$authfile"; fi
    if ! RUN_TIMEOUT=30m run podman pull --quiet --authfile "$authfile" --arch "$arch" "$REGISTRY_IMAGE:$tag" >"$tmp/id" 2>"$err"; then
        if grep -qiE 'unauthorized|authentication required|denied|invalid status code from registry 40[13]' "$err"; then
            log "the registry refused the pull of $REGISTRY_IMAGE:$tag: $(tr '\n' ' ' <"$err")"
            rm -rf -- "$tmp"
            if [[ -n $REGISTRY_AUTHFILE ]]; then
                fail "renew the registry credential: podman login --authfile $REGISTRY_AUTHFILE ghcr.io"
            fi
            fail "$REGISTRY_IMAGE needs a credential: set REGISTRY_AUTHFILE in deploy.env to a podman auth file with read access to it"
        fi
        log "could not pull $REGISTRY_IMAGE:$tag for $arch: $(tr '\n' ' ' <"$err")"
        rm -rf -- "$tmp"
        return 1
    fi
    id=$(tail -1 "$tmp/id")
    rm -rf -- "$tmp"
    [[ -n $id ]] || fail "podman pulled $REGISTRY_IMAGE:$tag for $arch but named no image"
    run podman tag "$id" "$name" || fail "could not tag the pulled image $id as $name"
    # Only the local name stays, so an old release's image goes once the cleanup drops that name.
    if run podman image exists "$REGISTRY_IMAGE:$tag" 2>/dev/null; then
        run podman untag "$id" "$REGISTRY_IMAGE:$tag" || true
    fi
}

# Stops the script, before it changes anything, when deploy.env names a registry credential that
# isn't there.
require_credential() {
    [[ -z $REGISTRY_AUTHFILE || -s $REGISTRY_AUTHFILE ]] ||
        fail "REGISTRY_AUTHFILE names $REGISTRY_AUTHFILE, which is missing or empty: log in with read access to $REGISTRY_IMAGE (podman login --authfile $REGISTRY_AUTHFILE ghcr.io), or drop the line for a public image"
}

# Keeps the image that runs now as :pre-trellis, with its revision in pre-trellis-rev, when that
# revision was never released: the first deploy of a release replaces a build from before them,
# and that image is the only one a restore can rebuild those revisions with. Does nothing after.
keep_pre_release_image() {
    local installed
    installed=$(cat "$APP/installed-rev" 2>/dev/null || true)
    if [[ ! -s $APP/pre-trellis-rev && -n $installed && -z $(release_of "$installed") ]] &&
        run podman image exists "$IMAGE:current"; then
        run podman tag "$IMAGE:current" "$IMAGE:$PRE_RELEASE_TAG"
        printf '%s\n' "$installed" >"$APP/pre-trellis-rev"
        log "kept the image of the unreleased revision $installed as $IMAGE:$PRE_RELEASE_TAG"
    fi
}

# Prints a unit or page from the kit with the deployment's names, address and ports from
# deploy.env. The scripts are copied as they are, and read the same file. The binary's path and the
# repository's name stay as they are.
render() {
    sed -e "s|/usr/local/bin/skyblock-collector|/usr/local/bin/@BINARY@|g" \
        -e "s|andvari-labs/skyblock-collector|andvari-labs/@REPOSITORY@|g" \
        -e "s/skyblock-collector/$NAME/g" \
        -e "s/@BINARY@\|@REPOSITORY@/skyblock-collector/g" \
        -e "s/127\.0\.0\.1:3004:8080/$BIND_ADDRESS:$STATUS_PORT:8080/" \
        -e "s/127\.0\.0\.1:5432:5432/$BIND_ADDRESS:$PG_PORT:5432/" "$1"
}

# The addresses and ports a pod unit publishes. The pod applies them only when it starts, so a
# change to them, from deploy.env, needs a pod restart.
published_ports() { grep '^PublishPort=' "$1" 2>/dev/null || true; }

# Restarts the pod, which Postgres and the collector run in, and waits for Postgres.
restart_pod() {
    log "$1; restarting the pod"
    run systemctl --user restart "$POD_SERVICE" || return 1
    run systemctl --user start "$DB_SERVICE" || return 1
    (wait_db)
}

link_units() {
    local quadlet_dir="$HOME/.config/containers/systemd/$NAME"
    local unit_dir="$HOME/.config/systemd/user"
    mkdir -p "$quadlet_dir" "$unit_dir/$NAME-pod.service.d"
    local q u
    for q in "${QUADLETS[@]}"; do ln -sfn "$APP/quadlet/${q/skyblock-collector/$NAME}" "$quadlet_dir/${q/skyblock-collector/$NAME}"; done
    for u in "${UNITS[@]}"; do ln -sfn "$APP/systemd/${u/skyblock-collector/$NAME}" "$unit_dir/${u/skyblock-collector/$NAME}"; done
    ln -sfn "$APP/systemd/$NAME-pod.service.d/restart.conf" "$unit_dir/$NAME-pod.service.d/restart.conf"
    run systemctl --user daemon-reload
}

# Copies the kit from a checkout into $APP. Each file is replaced by rename, so a script that is
# running keeps reading its old copy.
install_deploy_files() {
    local src=$1 f
    mkdir -p "$APP/quadlet" "$APP/systemd/$NAME-pod.service.d" "$APP/postgres" "$SECONDARY_DATA"
    place() {
        if [[ ${4:-} == render ]]; then render "$1" >"$2.new.$$"; else cp -- "$1" "$2.new.$$"; fi
        chmod "$3" "$2.new.$$"
        mv -f -- "$2.new.$$" "$2"
    }
    for f in "${QUADLETS[@]}"; do place "$src/quadlet/$f" "$APP/quadlet/${f/skyblock-collector/$NAME}" 600 render; done
    for f in "${UNITS[@]}"; do place "$src/systemd/$f" "$APP/systemd/${f/skyblock-collector/$NAME}" 600 render; done
    place "$src/systemd/skyblock-collector-pod.service.d/restart.conf" "$APP/systemd/$NAME-pod.service.d/restart.conf" 600 render
    # Postgres reads pg_hba.conf as uid 999, which keep-id maps to the host user.
    place "$src/postgres/pg_hba.conf" "$APP/postgres/pg_hba.conf" 644
    for f in "${SCRIPTS[@]}"; do place "$src/$f" "$APP/$f" 700; done
    place "$src/README.md" "$APP/README.md" 600 render
}

# Runs a one-off collector command against the database, with no network; the database is reached
# over its Unix socket. The archive is mounted read-only unless ARCHIVE_MODE=rw; the files pulled
# from the secondary collector are mounted read-only at /data/secondary. The image runs as uid 999,
# which keep-id maps to the host user, as the collector's own unit does.
collector_cmd() {
    mkdir -p "$SECONDARY_DATA"
    run podman run --rm --pull=never --network=none --userns=keep-id:uid=999,gid=999 --user=999:999 \
        --read-only --cap-drop=all --security-opt=no-new-privileges \
        --env-file "${COLLECTOR_ENV_FILE:-$APP/collector.env}" \
        -v "$APP/data/socket:/run/postgresql" \
        -v "$APP/data/archive:/data/archive:${ARCHIVE_MODE:-ro}" \
        -v "$SECONDARY_DATA:/data/secondary:ro" \
        "${COLLECTOR_IMAGE:-$IMAGE:current}" "$@"
}
