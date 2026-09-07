#!/bin/bash
# sync-bootstraps.sh - mirror the bootstrap tree from the file master out to the
# hosts that actually serve it.
#
# Deployment specifics (target hosts, key, paths) come from the environment so
# this file carries no site topology. Set them in the unit or cron environment.
#
# SCHEDULE: hourly, and it must stay at least that frequent.
#
# THIS SCRIPT IS THE ONLY PATH FROM THE FILE MASTER TO THE SERVING TIER. The
# master is not in the serving path: the bootstrap hostname resolves to the web
# tier, and the master's own docroot entry is a symlink to the payload tree, so
# a fresh archive is visible there while the public still cannot get it.
# Inspecting the master therefore proves nothing about what is served, and
# neither does the publisher's success line, which only means "reached the
# master".
#
# This ran daily for a while, in a slot that happened to fall about an hour
# BEFORE the monthly tracker publish began. The publish runs for hours, so every
# month this fired against the previous month's tree, exited rc=0, and left the
# public tier advertising a month-old bootstrap until the next day's run. Do not
# move it back to a daily slot, and do not pick a slot you believe is "after"
# the publish: that window is hours long and grows with the chain, so frequency
# is the guarantee, timing is not.
#
# RETENTION IS NOT COORDINATED with the master. The publisher prunes to KEEP
# archives at publish time, so between that prune and the next mirror pass the
# serving tier is the ONLY holder of the superseded archive while still
# advertising links to it. --delete-after is what finally retires them.
#
# Notes:
#   - --partial-dir keeps a resumable partial OUT of the served tree, so a
#     download can never hit a half-written multi-GB tarball; the finished file
#     appears by atomic rename. It only protects an INTERRUPTED transfer: rsync
#     writes to a hidden .<name>.XXXXXX temp while running and moves it into
#     .rsync-partial on a clean interrupt, so a hard kill (SIGKILL, or a SIGTERM
#     it cannot service) strands that temp and the next run restarts the file
#     from zero. The stray temp is extraneous and --delete-after sweeps it, but
#     the transfer work is lost.
#   - --exclude='*.part' because the publisher uploads each archive as
#     <name>.part and only then renames it into place. Without this, an hourly
#     pass landing inside the publish window hauls a partial upload of up to
#     ~160 GB to every target and deletes it on the next pass. --partial-dir
#     does NOT cover this: those are the PUBLISHER's temp files sitting in the
#     source tree, not rsync's own.
#   - --bwlimit protects the master, which also carries the hub and the
#     replication master. Raise it deliberately, not by default.
#   - --delete keeps the mirror exact. Safe here because the source holds one
#     snapshot per coin/network and the browser assets live in the same tree.
#   - The SITE leg mirrors the docroot-level chrome (index.html, stylesheets,
#     assets/, listing templates) that the payload leg never touches. Without it
#     the tier serves the bootstraps page with no stylesheet and a 404ing logo.
#     `--exclude=$PAYLOAD_DIRNAME` keeps the source's symlink from clobbering
#     the targets' real payload directory, and --delete never removes an
#     excluded path, so the payload tree is safe from this leg.
set -u

# --- deployment configuration (override in the environment) ------------------
SRC="${BOOTSTRAP_SRC:?set BOOTSTRAP_SRC to the payload tree on the file master, with trailing slash}"
SITE_SRC="${BOOTSTRAP_SITE_SRC:?set BOOTSTRAP_SITE_SRC to the docroot on the file master, with trailing slash}"
DEST_PATH="${BOOTSTRAP_DEST:?set BOOTSTRAP_DEST to the payload path on the targets, with trailing slash}"
SITE_DEST="${BOOTSTRAP_SITE_DEST:?set BOOTSTRAP_SITE_DEST to the docroot on the targets, with trailing slash}"
TARGETS="${BOOTSTRAP_TARGETS:?set BOOTSTRAP_TARGETS to a space-separated list of serving hosts}"
SSH_KEY="${BOOTSTRAP_SSH_KEY:?set BOOTSTRAP_SSH_KEY to the private key authorised on the targets}"
REMOTE_USER="${BOOTSTRAP_REMOTE_USER:-www}"
PAYLOAD_DIRNAME="${BOOTSTRAP_PAYLOAD_DIRNAME:-bootstraps}"
BWLIMIT="${BWLIMIT:-60M}"
LOCKFILE="${BOOTSTRAP_LOCKFILE:-/var/tmp/sync-bootstraps.lock}"
LOGFILE="${BOOTSTRAP_LOGFILE:-/var/log/sync-bootstraps.log}"

log(){ echo "[$(date -u '+%F %T UTC')] $*" >> "$LOGFILE"; }

# PID lock with stale-lock recovery: a crashed run must not block every future
# one.
if [ -f "$LOCKFILE" ]; then
    OLDPID=$(cat "$LOCKFILE" 2>/dev/null)
    if [ -n "$OLDPID" ] && kill -0 "$OLDPID" 2>/dev/null; then
        log "already running as pid $OLDPID; exiting"
        exit 0
    fi
    log "clearing stale lock (pid ${OLDPID:-unknown} is gone)"
fi
echo $$ > "$LOCKFILE"
trap 'rm -f "$LOCKFILE"' EXIT

if [ ! -d "$SRC" ]; then
    log "FATAL: source $SRC missing"
    exit 1
fi
if [ ! -d "$SITE_SRC" ]; then
    log "FATAL: site source $SITE_SRC missing"
    exit 1
fi

log "=== start (bwlimit=$BWLIMIT, source $(du -sh "$SRC" 2>/dev/null | cut -f1)) ==="
RC_TOTAL=0
for T in $TARGETS; do
    log "--- $T: site files starting"
    START=$(date +%s)
    rsync -a --delete-after \
          --exclude="$PAYLOAD_DIRNAME" \
          --exclude=.rsync-partial \
          --stats \
          -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=accept-new" \
          "$SITE_SRC" "${REMOTE_USER}@${T}:${SITE_DEST}" >> "$LOGFILE" 2>&1
    RC=$?
    ELAPSED=$(( $(date +%s) - START ))
    if [ $RC -eq 0 ]; then
        log "--- $T: site files OK in ${ELAPSED}s"
    else
        log "--- $T: site files FAILED rc=$RC after ${ELAPSED}s"
        RC_TOTAL=$RC
    fi

    log "--- $T: starting"
    START=$(date +%s)
    rsync -a --delete-after \
          --partial-dir=.rsync-partial \
          --exclude='*.part' \
          --bwlimit="$BWLIMIT" \
          --stats \
          -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=accept-new" \
          "$SRC" "${REMOTE_USER}@${T}:${DEST_PATH}" >> "$LOGFILE" 2>&1
    RC=$?
    ELAPSED=$(( $(date +%s) - START ))
    if [ $RC -eq 0 ]; then
        log "--- $T: OK in ${ELAPSED}s"
    else
        log "--- $T: FAILED rc=$RC after ${ELAPSED}s"
        RC_TOTAL=$RC
    fi
done
log "=== done (worst rc=$RC_TOTAL) ==="
exit $RC_TOTAL
