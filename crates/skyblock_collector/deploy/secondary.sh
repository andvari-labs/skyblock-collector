#!/usr/bin/env bash
# Deploys the secondary collector on a revision to $SECONDARY_HOST.
# Usage: secondary.sh <revision> <checkout of that revision>
#
# The host is ARM, so this pulls the ARM64 image of the revision's release here and copies it over;
# the host needs no registry credential. Each deploy that passes its
# health check becomes the known-good one: its image is tagged :good and its unit saved as .good.
# If a new deploy is not healthy within five minutes, the known-good image and unit go back, and
# the revision is recorded in secondary-bad-rev so update.sh does not retry it. update.sh runs
# this after each deploy; any other failure leaves the main collector as it is and is retried
# within the hour.
# shellcheck source=common.sh source-path=SCRIPTDIR
source "$(dirname "$(realpath "$0")")/common.sh"

revision=${1:?usage: secondary.sh <revision> <checkout>}
src=$(realpath "${2:?usage: secondary.sh <revision> <checkout>}")
image="$SECONDARY_IMAGE:$revision"

tag=$(release_of "$revision")
[[ -n $tag ]] || fail "$revision is not a release, so it has no image to deploy"
log "pulling the secondary image for $tag ($revision)"
pull_release "$tag" "$SECONDARY_ARCH" "$image" || fail "could not pull the $SECONDARY_ARCH image for $tag"

log "copying the secondary image to $SECONDARY_HOST"
run podman save "$image" | secondary_ssh podman load --quiet >/dev/null
secondary_ssh mkdir -p "$SECONDARY_APP/data/archive" "$SECONDARY_APP/logs" .config/containers/systemd
unit=$SECONDARY_UNIT
new_id=$(secondary_ssh podman image inspect --format '{{.Id}}' "$image")
good_id=$(secondary_ssh podman image inspect --format '{{.Id}}' "$SECONDARY_IMAGE:good" 2>/dev/null || true)

secondary_ssh podman tag "$image" "$SECONDARY_IMAGE:current"
render "$src/$KIT_PATH/secondary/skyblock-collector-secondary.container" | secondary_ssh "cat > $unit.new && mv -f $unit.new $unit"
# Succeeds when the service restarted. A failed restart can leave the old container running and
# healthy, so it counts as unhealthy, not as something to check. ssh's own exit 255 is a lost
# connection, which proves nothing either way.
restart() {
    local rc=0
    secondary_ssh "systemctl --user daemon-reload && systemctl --user restart $SECONDARY_SERVICE" || rc=$?
    ((rc != 255)) || fail "lost the connection to $SECONDARY_HOST while deploying $revision"
    ((rc == 0)) || log "the secondary collector did not restart (exit $rc)"
    return "$rc"
}

healthy() {
    local i
    for ((i = 0; i < 60; i++)); do
        secondary_ssh podman healthcheck run "$SECONDARY_CONTAINER" >/dev/null 2>&1 && return 0
        sleep 5
    done
    return 1
}
if restart && healthy; then
    secondary_ssh podman tag "$image" "$SECONDARY_IMAGE:good"
    secondary_ssh cp -p "$unit" "$unit.good"
    # Keep only the deployed image there and here. rmi of a name that another name shares only
    # removes the name. podman lists every name of each image it matches, names in other
    # repositories too. Only this repository's names are the kit's to remove, and the registry
    # names a pull leaves when its untag fails.
    remote_list=$(secondary_ssh podman images --format '{{.Repository}}:{{.Tag}}' "$SECONDARY_IMAGE")
    mapfile -t remote_refs <<<"$remote_list"
    for ref in "${remote_refs[@]}"; do
        case $ref in
            "$SECONDARY_IMAGE":current | "$SECONDARY_IMAGE":good) ;;
            "$SECONDARY_IMAGE":*) secondary_ssh podman rmi "$ref" </dev/null >/dev/null 2>&1 || true ;;
        esac
    done
    # Remove the old known-good image by ID only when no name at all keeps it: rmi by ID deletes
    # an image that a name outside the kit still holds.
    if [[ -n $good_id && $good_id != "$new_id" &&
        $(secondary_ssh podman image inspect --format '{{.RepoTags}}' "$good_id" </dev/null 2>/dev/null) == '[]' ]]; then
        secondary_ssh podman rmi "$good_id" </dev/null >/dev/null 2>&1 || true
    fi
    local_list=$(run podman images --format '{{.Repository}}:{{.Tag}}' "$SECONDARY_IMAGE")
    mapfile -t local_refs <<<"$local_list"
    for ref in "${local_refs[@]}"; do
        case $ref in
            "$image") ;;
            "$SECONDARY_IMAGE":* | "$REGISTRY_IMAGE":*) run podman rmi "$ref" >/dev/null 2>&1 || true ;;
        esac
    done
    printf '%s\n' "$revision" >"$APP/secondary-rev"
    rm -f -- "$APP/secondary-bad-rev"
    log "deployed $revision to $SECONDARY_HOST"
    exit 0
fi

# Without a connection the health check proves nothing; leave it for the next try.
secondary_ssh true || fail "lost the connection to $SECONDARY_HOST while deploying $revision"
log "the secondary collector is unhealthy on $revision; rolling back"
printf '%s\n' "$revision" >"$APP/secondary-bad-rev"
[[ -n $good_id ]] || fail "the secondary collector failed on $revision, and no earlier deploy passed its health check"
secondary_ssh cp -p "$unit.good" "$unit"
secondary_ssh podman tag "$SECONDARY_IMAGE:good" "$SECONDARY_IMAGE:current"
{ restart && healthy; } || fail "the known-good secondary image is unhealthy too; check $SECONDARY_HOST"
if [[ $new_id != "$good_id" ]]; then
    # The known-good image runs now, so this removes the failed one, not only its name.
    secondary_ssh podman rmi "$image" >/dev/null 2>&1 || true
fi
fail "the secondary collector failed on $revision; it runs the known-good image again"
