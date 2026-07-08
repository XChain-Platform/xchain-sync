<?php
/**
 * sync.xchain.io - resolve "latest.tgz" to the newest published bootstrap.
 *
 * Deploy to: /var/www/virtual/sync.xchain.io/bootstraps/latest.php
 * Driven by the sibling .htaccess, which rewrites
 *   <service>/<coin>/<network>/latest.tgz        -> latest.php?dir=...&type=tgz
 *   <service>/<coin>/<network>/latest.tgz.sha256 -> latest.php?dir=...&type=sha256
 *   <service>/<coin>/<network>/latest.tgz.sig    -> latest.php?dir=...&type=sig
 *
 * Bootstrap archives are named  <network>-<service>-<YYYYMMDD_HHMMSS>.tar.gz
 * (xchain-node BootstrapService). That timestamp suffix is lexically
 * sortable, so the newest archive is just the max filename; mtime is the
 * tie-breaker / fallback for oddly-named files.
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

$newest = $files[0];
$name   = basename($newest);

if ($type === 'sha256') {
    $sha = $newest . '.sha256';
    if (!is_file($sha)) {
        http_response_code(404);
        header('Content-Type: text/plain');
        exit("No checksum published\n");
    }
    header('Content-Type: text/plain');
    readfile($sha);
    exit;
}

if ($type === 'sig') {
    // Detached signature published next to the SAME newest archive. A 404 here
    // means the newest archive is unsigned (xchain-node then treats the
    // bootstrap as unsigned per its signature policy).
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
