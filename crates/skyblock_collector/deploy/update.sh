#!/usr/bin/env bash
# Deploys the newest release when it differs from the installed revision. Usage: update.sh [--force]
#
# Fetches the release tags, pulls that release's image, swaps the image and the deploy files from
# the same tag, and restarts the collector. If the collector is not healthy within five minutes, it
# puts back the previous image and files and records the revision as failed, so it is skipped until
# a newer release. Postgres restarts only when its own unit file changed. Then it deploys the
# secondary collector; a failure there leaves the main collector running and is retried hourly,
# except for a revision the secondary was unhealthy on, which waits for the next release. A release
# whose image cannot be pulled yet (the release workflow pushes it after the tag) is tried again on
# the next run and not marked failed.
# shellcheck source=common.sh source-path=SCRIPTDIR
source "$(dirname "$(realpath "$0")")/common.sh"

# Installs the pull timer and deploys the secondary collector for a revision checked out at $2.
deploy_secondary() {
    if [[ -z $SECONDARY_HOST ]]; then
        run systemctl --user disable --now "$NAME-pull.timer" 2>/dev/null || true
        return 0
    fi
    install_deploy_files "$2/$KIT_PATH"
    link_units
    if [[ $ENABLE_TIMERS == 1 ]]; then run systemctl --user enable --now "$NAME-pull.timer"; fi
    if "$APP/secondary.sh" "$1" "$2"; then
        rm -f -- "$APP/secondary-failed"
    else
        date +%s >"$APP/secondary-failed"
        log "the secondary collector is not on $1; retrying within the hour"
    fi
}

force=${1:-}
lock
# The deploy that installed this kit ran the older kit's update.sh, which linked the units again;
# the first run of this one replaces those links with copies, keeping each timer's state.
if units_linked; then
    log 'replacing the links to the systemd units with copies'
    link_units
fi
# Without a secondary collector there is nothing to pull; a timer left from one would fail every run.
if [[ -z $SECONDARY_HOST ]] && systemctl --user is-enabled --quiet "$NAME-pull.timer" 2>/dev/null; then
    run systemctl --user disable --now "$NAME-pull.timer" || true
fi
tag=$(newest_release)
[[ -n $tag ]] || {
    log 'no release yet; nothing to deploy'
    exit 0
}
target=$(git -C "$REPO" rev-parse "$tag^{commit}")
installed=$(cat "$APP/installed-rev" 2>/dev/null || true)
if [[ $force != --force ]]; then
    if [[ $target == "$installed" ]]; then
        failed_at=$(cat "$APP/secondary-failed" 2>/dev/null || echo 0)
        if [[ -n $SECONDARY_HOST && $(cat "$APP/secondary-rev" 2>/dev/null || true) != "$installed" &&
            $(cat "$APP/secondary-bad-rev" 2>/dev/null || true) != "$installed" ]] &&
            (($(date +%s) - failed_at >= 3600)); then
            WORK=$(mktemp -d)
            trap 'rm -rf -- "$WORK"' EXIT
            run git -C "$REPO" archive "$installed" | tar -x -C "$WORK"
            deploy_secondary "$installed" "$WORK"
        fi
        exit 0
    fi
    if [[ $target == "$(cat "$APP/failed-rev" 2>/dev/null || true)" ]]; then
        log "skipping $tag ($target): its deploy failed; release a fix"
        exit 0
    fi
fi
log "deploying $tag ($target; installed: ${installed:-none})"

WORK=$(mktemp -d)
trap 'rm -rf -- "$WORK"' EXIT
run git -C "$REPO" archive "$target" | tar -x -C "$WORK"
if ! pull_release "$tag" amd64 "$IMAGE:$target"; then
    # The release workflow pushes the image minutes after the tag. One still missing hours later
    # never will be, such as after a failed image job or a tag pushed by hand, and waiting on it
    # would hold back every later release without a sound.
    # Measured from this host's first miss, since a tag can be pushed long after its commit.
    read -r missing_tag missing_since 2>/dev/null <"$APP/missing-image" || true
    if [[ ${missing_tag:-} != "$tag" ]]; then
        printf '%s %s\n' "$tag" "$(date +%s)" >"$APP/missing-image"
    elif (($(date +%s) - missing_since > MISSING_IMAGE_LIMIT_SECS)); then
        printf '%s\n' "$target" >"$APP/failed-rev"
        rm -f -- "$APP/missing-image"
        fail "$tag has had no image for over $((MISSING_IMAGE_LIMIT_SECS / 3600)) hours; it is marked failed. Re-run its release workflow's image job, then update.sh --force"
    fi
    log "the image for $tag is not available yet; trying again on the next run"
    exit 0
fi
rm -f -- "$APP/missing-image"
run git -C "$REPO" checkout --quiet --detach "$target"

# Keep what is running now, to put back on failure.
saved="$WORK/saved"
mkdir -p "$saved"
cp -a -- "$APP/quadlet" "$APP/systemd" "$APP/postgres" "$saved/"
# A script this revision adds is not installed yet when an older update.sh made the last deploy.
for f in "${SCRIPTS[@]}"; do
    if [[ -e $APP/$f ]]; then cp -a -- "$APP/$f" "$saved/"; fi
done
db_unit="$APP/quadlet/$NAME-db.container"
db_unit_before=$(sha256sum "$db_unit" | cut -d' ' -f1)
pod_unit="$APP/quadlet/$NAME.pod"
ports_before=$(published_ports "$pod_unit")
previous_image=$(run podman image inspect --format '{{.Id}}' "$IMAGE:current" 2>/dev/null || true)

keep_pre_release_image
install_deploy_files "$WORK/$KIT_PATH"
link_units
run podman tag "$IMAGE:$target" "$IMAGE:current"
pod_restarted=0
started=1
if [[ $(published_ports "$pod_unit") != "$ports_before" ]]; then
    pod_restarted=1
    # A pod that can't start on the new address, such as one the host doesn't hold, rolls back.
    restart_pod 'the published address or ports changed' || started=0
elif [[ $(sha256sum "$db_unit" | cut -d' ' -f1) != "$db_unit_before" ]]; then
    log 'the Postgres unit changed; restarting Postgres'
    run systemctl --user restart "$DB_SERVICE"
    wait_db
fi
if ((started)); then run systemctl --user restart "$APP_SERVICE" || started=0; fi

if ((started)) && wait_collector; then
    printf '%s\n' "$target" >"$APP/installed-rev"
    rm -f -- "$APP/failed-rev"
    [[ -n $previous_image ]] && run podman tag "$previous_image" "$IMAGE:previous"
    # Keep the current and previous images, and the image a restore can rebuild an unreleased
    # revision with; remove older revision tags. podman lists every name of each image it matches,
    # names in other repositories too. Only this repository's names are the kit's to remove, and
    # the registry names a pull leaves when its untag fails.
    run podman images --format '{{.Repository}}:{{.Tag}} {{.Id}}' "$IMAGE" | while read -r ref id; do
        case $ref in
            "$IMAGE":current | "$IMAGE":previous | "$IMAGE":"$PRE_RELEASE_TAG") ;;
            "$IMAGE":* | "$REGISTRY_IMAGE":*) [[ $id == "$previous_image" || $ref == "$IMAGE:$target" ]] || run podman rmi "$ref" >/dev/null 2>&1 || true ;;
        esac
    done
    log "deployed $tag ($target)"
    deploy_secondary "$target" "$WORK"
    exit 0
fi

log "the collector is unhealthy on $tag ($target); rolling back"
printf '%s\n' "$target" >"$APP/failed-rev"
rendered_db_unit=$(render "$WORK/$KIT_PATH/quadlet/skyblock-collector-db.container" | sha256sum | cut -d' ' -f1)
cp -a -- "$saved/quadlet" "$saved/systemd" "$saved/postgres" "$APP/"
for f in "${SCRIPTS[@]}"; do
    if [[ -e $saved/$f ]]; then
        cp -- "$saved/$f" "$APP/$f.new.$$" && mv -f -- "$APP/$f.new.$$" "$APP/$f"
    else
        rm -f -- "$APP/$f"
    fi
done
link_units
if [[ -n $previous_image ]]; then
    run podman tag "$previous_image" "$IMAGE:current"
fi
if ((pod_restarted)); then
    restart_pod 'putting back the published address and ports that ran before' || log 'the pod did not restart on the address and ports that ran before'
elif [[ $(sha256sum "$db_unit" | cut -d' ' -f1) != "$rendered_db_unit" ]]; then
    run systemctl --user restart "$DB_SERVICE"
    wait_db
fi
run systemctl --user restart "$APP_SERVICE"
if [[ -n $installed ]]; then
    run git -C "$REPO" checkout --quiet --detach "$installed" 2>/dev/null ||
        log "the clone lacks the installed revision $installed; it stays at $target"
fi
wait_collector || fail "rollback to ${installed:-the previous image} is unhealthy too; manual recovery needed"
fail "rolled back to ${installed:-the previous image}; $tag ($target) is marked failed"
