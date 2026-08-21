<?php
/**
 * sync.xchain.io - resolve "latest.tgz" to the newest published bootstrap.
 *
 * Deploy to: /var/www/virtual/sync.xchain.io/bootstraps/latest.php
 * Driven by the sibling .htaccess, which rewrites
 *   <service>/<coin>/<network>/latest.tgz     -> latest.php?dir=...&type=tgz
 *   <service>/<coin>/<network>/latest.tgz.sig -> latest.php?dir=...&type=sig
 *
 * Bootstrap archives are named  <network>-<service>-<YYYYMMDD_HHMMSS>.tar.gz
 * (xchain-node BootstrapService). That timestamp suffix is lexically
 * sortable, so the newest archive is just the max filename; mtime is the
 * tie-breaker / fallback for oddly-named files.
 *
 * "Latest" means the newest archive that HAS a paired detached signature
 * (<archive>.sig), never merely the newest by filename. See the resolver
 * below for why unsigned archives are skipped rather than advertised. This
 * is the canonical signed-latest resolver, kept in lockstep with the
 * xchain-websites copy at sync.xchain.io/bootstraps-app/latest.php.
 *
 * There is no external <archive>.sha256 sidecar: BootstrapService embeds the
 * checksum INSIDE the signature-verified outer archive (data.sha256), so the
 * detached .sig is the sole published integrity anchor and a separate unsigned
 * .sha256 endpoint would add nothing but a permanently-404 route.
 *
 * We 302-redirect to the real file rather than stream it: archives are
 * tens to hundreds of GB, so Apache must serve them directly to keep
 * Range/resume support. The xchain-node downloader follows redirects and
 * treats 404 as "nothing published yet".
 */

$base = __DIR__;                                  // .../sync.xchain.io/bootstraps
$dir  = isset($_GET['dir'])  ? $_GET['dir']  : '';
$type = isset($_GET['type']) ? $_GET['type'] : 'tgz';

// Validate the directory: slash-separated segments of [A-Za-z0-9._-], no traversal.
if (!preg_match('#^[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$#', $dir)
    || strpos($dir, '..') !== false) {
    http_response_code(400);
    header('Content-Type: text/plain');
    exit("Bad request\n");
}

$target = realpath($base . '/' . $dir);
if ($target === false
    || strpos($target, $base . DIRECTORY_SEPARATOR) !== 0
    || !is_dir($target)) {
    http_response_code(404);
    header('Content-Type: text/plain');
    exit("Not found\n");
}

// A hand-placed real latest.tgz beats this resolver on the archive route: the
// sibling .htaccess rewrites latest.tgz only when no real file by that name
// exists, so Apache streams the drop and the consumer sees no redirect. The
// signature route must then describe THAT file and must never fall through to
// the newest timestamped archive below. Handing the drop's bytes another
// archive's .sig makes the consumer report tampering on data nobody tampered
// with, and its unsigned-bootstrap opt-out cannot rescue it, because that
// opt-out covers an ABSENT signature and this one is present, merely wrong. So
// serve the drop's own sibling .sig (reached only when latest.php is called
// directly; Apache serves a real one itself), else 404 so the consumer takes
// its documented no-signature-published path.
if ($type === 'sig' && is_file($target . '/latest.tgz')) {
    $manual_sig = $target . '/latest.tgz.sig';
    if (!is_file($manual_sig)) {
        http_response_code(404);
        header('Content-Type: text/plain');
        exit("No signature published\n");
    }
    header('Content-Type: text/plain');
    readfile($manual_sig);
    exit;
}

// Candidate archives: *.tar.gz and *.tgz, excluding the latest.* aliases.
$files = array_merge(
    glob($target . '/*.tar.gz') ?: [],
    glob($target . '/*.tgz')    ?: []
);
$files = array_values(array_filter($files, function ($f) {
    $b = basename($f);
    return $b !== 'latest.tgz' && $b !== 'latest.tar.gz';
}));

if (empty($files)) {
    http_response_code(404);
    header('Content-Type: text/plain');
    exit("No bootstrap published\n");
}

// Newest first: descending by filename (timestamp suffix), mtime breaks ties.
usort($files, function ($a, $b) {
    $c = strcmp(basename($b), basename($a));
    if ($c !== 0) return $c;
    return filemtime($b) <=> filemtime($a);
});

// Security: the advertised "latest" must be the newest archive that HAS a
// paired detached signature (<archive>.sig), NOT merely the newest by filename.
// Otherwise one unsigned publish - a newer archive dropped in without its .sig,
// or a partial scp where the .sig never landed - becomes THE latest: every
// fresh install fetches it, then 404s on the .sig fetch and (fail-closed)
// aborts to a full resync, even while a good signed archive sits right
// alongside. So skip unsigned archives and pick the newest signed one; if none
// is signed, there is nothing installable to advertise -> 404. The tgz and sig
// endpoints resolve the same archive here, and the hand-placed-drop guard above
// keeps that true on the one route that bypasses this resolver, so they can
// never drift apart.
$newest = null;
foreach ($files as $f) {
    if (is_file($f . '.sig')) { $newest = $f; break; }
}
if ($newest === null) {
    http_response_code(404);
    header('Content-Type: text/plain');
    exit("No signed bootstrap published\n");
}
$name = basename($newest);

if ($type === 'sig') {
    // Detached signature for the SAME archive the tgz endpoint serves. The
    // selection above guarantees this file exists; the guard covers only a
    // concurrent prune racing between the two calls.
    $sig = $newest . '.sig';
    if (!is_file($sig)) {
        http_response_code(404);
        header('Content-Type: text/plain');
        exit("No signature published\n");
    }
    header('Content-Type: text/plain');
    readfile($sig);
    exit;
}

// 302 to the real archive (absolute path; filename chars are already safe).
header('Location: /bootstraps/' . $dir . '/' . $name, true, 302);
header('Cache-Control: no-cache, must-revalidate');
exit;
