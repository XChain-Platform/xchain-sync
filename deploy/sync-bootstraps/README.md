# sync-bootstraps

Serving side of bootstrap distribution.

Two pieces, deployed to different machines:

| File | Deployed to | Typical path |
|---|---|---|
| `sync-bootstraps.sh` | the file master | `/usr/local/sbin/sync-bootstraps.sh`, mode 755, root-owned |
| `latest.php` | each serving host | inside the served payload tree, with its `.htaccess` |

`latest.php` resolves `latest.tgz` / `latest.tgz.sig` to the newest archive that
has a paired signature, ordered by the UTC timestamp in the filename.

## The thing that makes this easy to get wrong

**The file master does not serve the bootstraps.** The public hostname resolves
to the serving tier, and the master's docroot entry for the payload directory is
a symlink to the real tree. So on the master a freshly published archive looks
published. It is not public until this script has mirrored it out.

The generators push only as far as the master and log a success line when they
get there. That line means "reached the master", not "reached the internet".
Verify against the serving tier, never the master.

## Cron

Install in root's crontab on the file master:

```cron
# Mirror the bootstrap tree out to the serving tier. Hourly, NOT daily: see the
# SCHEDULE block in the script. The PID lock makes an overlapping run a no-op.
30 * * * * /usr/local/sbin/sync-bootstraps.sh 1>/dev/null 2>/dev/null
```

Hourly is a correctness requirement, not tuning. The monthly tracker publish
runs for hours. A daily slot that happens to fall before it means the tier
serves the previous month's bootstrap until the next day's pass, with every
component reporting success. That is exactly what happened while this ran daily.

Do not replace hourly with a slot chosen to be "after" the publish. The publish
window grows with the chain, so frequency is the guarantee and timing is not.

## Environment

The script carries no site topology. Set these where cron or the unit can see
them; the script refuses to start if any required one is unset.

| Variable | Meaning |
|---|---|
| `BOOTSTRAP_SRC` | payload tree on the master, trailing slash |
| `BOOTSTRAP_SITE_SRC` | docroot on the master, trailing slash |
| `BOOTSTRAP_DEST` | payload path on the targets, trailing slash |
| `BOOTSTRAP_SITE_DEST` | docroot on the targets, trailing slash |
| `BOOTSTRAP_TARGETS` | space-separated serving hosts |
| `BOOTSTRAP_SSH_KEY` | private key authorised on the targets |
| `BOOTSTRAP_REMOTE_USER` | ssh user on the targets (default `www`) |
| `BOOTSTRAP_PAYLOAD_DIRNAME` | payload dir name, excluded from the site leg (default `bootstraps`) |
| `BWLIMIT` | rsync bandwidth cap (default `60M`) |
| `BOOTSTRAP_LOCKFILE`, `BOOTSTRAP_LOGFILE` | lock and log paths |

Use a dedicated key, not one shared with other fanouts, and restrict it to the
master's address in `authorized_keys`.

## Operating notes

- Targets are mirrored **sequentially**. A full pass is the size of the whole
  tree per target, so total wall time is roughly one target's transfer time
  times the number of targets.
- `BWLIMIT` throttles the master, which typically also carries the hub and the
  replication master. Raising it doubles throughput about linearly; raise it
  deliberately, and remember it applies per run, not per target.
- **Do not edit this script on the machine while a run is in flight.** bash
  reads a script lazily by byte offset, so an in-place edit can corrupt the
  running execution. Stop the run, edit, restart.
- Killing a run hard strands rsync's hidden `.<name>.XXXXXX` temp instead of
  moving it into `.rsync-partial`, so that file restarts from zero next pass.
  The stray temp is extraneous and `--delete-after` sweeps it.
- If you stop a run to restart it with different settings, confirm the old rsync
  is actually gone before relaunching. Two concurrent passes against the same
  target will both transfer, doubling load on the master. `pgrep` takes an
  extended regex, so a pattern like `'a\|b'` matches nothing and will tell you
  the process is gone when it is not. Check with `ps` and kill by PID.
- Retention is **not** coordinated with the master. The publisher prunes at
  publish time, so between that prune and the next mirror pass the serving tier
  is the only holder of the superseded archive and is still serving links to it.

## Verifying a publish actually reached the public

Check every address the hostname resolves to. A round-robin will otherwise mask
a host that missed the mirror:

```bash
HOST=<bootstrap hostname>
ARCHIVE=<coin>/<network>/<archive>.tar.gz
for ip in $(dig +short "$HOST"); do
  curl -s -o /dev/null -w "$ip %{http_code}\n" \
    --resolve "$HOST:443:$ip" \
    "https://$HOST/bootstraps/xchain-utxo-tracker/$ARCHIVE"
done
```

All addresses must return 200 for the same archive. Anything else means the
mirror has not finished, and consumers are getting different answers depending
on which host they reach.
