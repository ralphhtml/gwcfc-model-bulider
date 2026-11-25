#!/usr/bin/env python3
"""Runs the parsing server's small jobs here and uploads what they wrote, signed.

    python3 extras.py --group quick --loop 320     # lightning every 2 minutes, storm vitals and storm satellite every 10
    python3 extras.py --group slow                 # Waffle House, spaghetti plots, satellite winds, wind field, ACE (once)
    python3 extras.py --group slow --dry

Each job is the server's own program, run as it is run there. Before it starts, the small files the server has for
it are fetched (a job that keeps a little history carries on from it); afterwards whatever is new or changed is
uploaded, and the server is told this job is alive, which is what keeps its own copy of the job stood down.
"""
import argparse
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import tarfile
import tempfile
import time

import build as B

# job: (folder under the data directory, program, arguments, fetch the server's small files first)
JOBS = {
    "glm":         ("tropx", "tropx_pipeline.py", ["--glm"], False),
    "tropx":       ("tropx", "tropx_pipeline.py", [], True, ["feeds"]),      # it also writes the ASCAT passes and the Great Lakes
    "stormsat":    ("stormsat", "stormsat_pipeline.py", [], True),
    "wafflehouse": ("wafflehouse", "wafflehouse_pipeline.py", [], True),
    "spaghetti":   ("spaghetti", "spaghetti_pipeline.py", [], False),
    "satwinds":    ("feeds", "satwinds_pipeline.py", [], False),
    "wind":        ("wind", "wind_field_pipeline.py", [], True),
    "ace":         ("ace", "ace_pipeline.py", [], True),
    # The hurricane hunters: the live flights, and the archive a slice at a time (days.json says which days are done).
    "recon":       ("recon", "recon_pipeline.py", [], True),
    # NHC's cones: also needs the files of the storms still being added to (seed_cones).
    "nhccones":    ("nhccones", "nhc_cones_pipeline.py", [], True),
    # Both do nothing unless their source has changed since the stamp in the files fetched first.
    "tornadoes":   ("tornadoes", "tornado_archive_pipeline.py", [], True),
    "hurritrack":  ("hurritrack", "hurritrack_pipeline.py", [], True),
    # One file that never changes: built only if the server does not have it.
    "climo":       ("climo", "climo_build.py", [], False),
    # The ocean: the days the server already has are stood in for by placeholders (seed_sst), so only new ones are drawn.
    "sst":         ("sst", "sst_pipeline.py", ["--budget", "1500"], True),
    # The other countries' radars (UK, Israel, Australia, Taiwan, Portugal, Cuba, Bermuda, the Dominican Republic,
    # Jamaica, India), the buoys and the Saharan dust text. The alert history stays on the server.
    "feeds":       ("feeds", "feeds_pipeline.py", [], True),
}
ENV = {
    "nhccones": {"NHCCONES_BUILDER": "1"},
    "sst": {"GWCFC_SST_PRUNE_PCT": "101"},
}
ONCE = {"climo": "ncep_1991_2020.npz"}
GROUPS = {
    "quick": [("glm", 120), ("tropx", 600), ("stormsat", 600), ("recon", 600), ("feeds", 600)],
    "slow":  [("wafflehouse", 0), ("spaghetti", 0), ("satwinds", 0), ("wind", 0), ("ace", 0), ("nhccones", 0)],
    "daily": [("tornadoes", 0), ("hurritrack", 0), ("climo", 0), ("sst", 0)],
}
OK_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.\-]{0,120}\.(png|json|webp|npz|csv|geojson|gif|u8\.gz|bin\.gz)$")   # gif, u8.gz: the feeds radars; bin.gz: the mrrl volumes
OK_DIR = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_\-]{0,60}$")
PART_BYTES = 6 * 1024 * 1024


def log(msg):
    print(time.strftime("%H:%M:%S", time.gmtime()), msg, flush=True)


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def seed_top(base, tree, root, sent):
    """The server's own small files of this folder (its top level only), so the job carries on from them."""
    import requests
    try:
        page = requests.get(f"{base}/{tree}/", timeout=60, headers={"User-Agent": "gwcfc-model-builder"}).text
    except Exception:                                   # noqa: BLE001
        return 0
    n = 0
    for name in re.findall(r'href="([^"/?]+\.json)"', page)[:80]:
        if not OK_NAME.match(name):
            continue
        try:
            r = requests.get(f"{base}/{tree}/{name}", timeout=60, headers={"User-Agent": "gwcfc-model-builder"})
        except Exception:                               # noqa: BLE001
            continue
        if r.status_code != 200 or len(r.content) > 8e6:
            continue
        with open(os.path.join(root, name), "wb") as fh:
            fh.write(r.content)
        sent[name] = hashlib.sha256(r.content).hexdigest()
        n += 1
    return n


# Folders of the feeds tree the builder must not send back: the alert history is added to on the server only.
FEEDS_SKIP = ("alerts-hist",)


def seed_feeds(base, root, sent):
    """The radar pictures the server already has, folder by folder, so each country's loop carries on from them
    (most of these sources only ever show their newest picture, and a loop is built up a picture at a time)."""
    import requests
    hdr = {"User-Agent": "gwcfc-model-builder"}
    try:
        page = requests.get(f"{base}/feeds/", timeout=60, headers=hdr).text
    except Exception:                                   # noqa: BLE001
        return 0
    n = 0
    for sub in re.findall(r'href="([A-Za-z0-9_-]+)/"', page):
        if sub in FEEDS_SKIP:
            continue
        try:
            listing = requests.get(f"{base}/feeds/{sub}/", timeout=60, headers=hdr).text
        except Exception:                               # noqa: BLE001
            continue
        os.makedirs(os.path.join(root, sub), exist_ok=True)
        for name in re.findall(r'href="([^"/?]+)"', listing)[:400]:
            if not OK_NAME.match(name):
                continue
            try:
                r = requests.get(f"{base}/feeds/{sub}/{name}", timeout=60, headers=hdr)
            except Exception:                           # noqa: BLE001
                continue
            if r.status_code != 200 or len(r.content) > 8e6:
                continue
            with open(os.path.join(root, sub, name), "wb") as fh:
                fh.write(r.content)
            sent[f"{sub}/{name}"] = hashlib.sha256(r.content).hexdigest()
            n += 1
    return n


def _get(url):
    import requests
    try:
        r = requests.get(url, timeout=90, headers={"User-Agent": "gwcfc-model-builder"})
        return r.content if r.status_code == 200 else None
    except Exception:                                   # noqa: BLE001
        return None


def seed_cones(base, root, sent):
    """The cone files of the storms not finished yet (the ones this run may add advisories to)."""
    try:
        with open(os.path.join(root, "state.json")) as fh:
            state = json.load(fh)
        with open(os.path.join(root, "index.json")) as fh:
            ids = [r["id"] for r in json.load(fh).get("storms") or []]
    except (OSError, ValueError, KeyError, TypeError):
        return 0
    os.makedirs(os.path.join(root, "storms"), exist_ok=True)
    n = 0
    for sid in ids:
        if (state.get(sid) or {}).get("done") or not OK_DIR.match(str(sid)):
            continue
        body = _get(f"{base}/nhccones/storms/{sid}.json")
        if body:
            with open(os.path.join(root, "storms", sid + ".json"), "wb") as fh:
                fh.write(body)
            sent[f"storms/{sid}.json"] = hashlib.sha256(body).hexdigest()
            n += 1
    return n


def seed_sst(root, sent):
    """A placeholder for every picture the server's list names, so the program skips those days and its list keeps
    them. Placeholders are never uploaded."""
    try:
        with open(os.path.join(root, "index.json")) as fh:
            srcs = json.load(fh).get("sources") or {}
    except (OSError, ValueError):
        return 0
    stub = b"\0" * 2048
    h = hashlib.sha256(stub).hexdigest()
    n = 0
    for source, e in srcs.items():
        for variant, v in (e.get("variants") or {}).items():
            if not (OK_DIR.match(source) and OK_DIR.match(variant)):
                continue
            d = os.path.join(root, source, variant)
            os.makedirs(d, exist_ok=True)
            for stamp in v.get("frames") or []:
                if re.fullmatch(r"\d{8}", str(stamp)) and not os.path.exists(os.path.join(d, stamp + ".png")):
                    with open(os.path.join(d, stamp + ".png"), "wb") as fh:
                        fh.write(stub)
                    sent[f"{source}/{variant}/{stamp}.png"] = h
                    n += 1
    return n


def changed(root, sent):
    """Files under root that are new or differ from what was last sent or fetched. Caches and raw downloads the
    job keeps for itself (folders named so) are not the site's and are left out."""
    out = []
    for dp, dn, fn in os.walk(root):
        dn[:] = [d for d in dn if OK_DIR.match(d) and "cache" not in d.lower() and d != "raw"]
        for n in fn:
            if not OK_NAME.match(n):
                continue
            p = os.path.join(dp, n)
            rel = os.path.relpath(p, root).replace(os.sep, "/")
            if rel.count("/") > 3:
                continue
            h = sha(p)
            if sent.get(rel) != h:
                out.append((rel, h))
    return sorted(out, key=lambda x: (x[0].count("/") == 0, x[0]))      # the top-level lists last


def upload(base, key_path, tree, root, job, files, sent, keep_hours=None):
    run = time.strftime("%Y%m%d_%H", time.gmtime())
    batch, size, part = [], 0, 0

    def flush():
        nonlocal batch, size, part
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tf:
            for rel, _h in batch:
                tf.add(os.path.join(root, rel), arcname=rel, recursive=False)
        B.post(base, key_path, "tree", tree, run, "part", part, buf.getvalue())
        part += 1
        batch, size = [], 0
    for rel, h in files:
        s = os.path.getsize(os.path.join(root, rel))
        if batch and size + s > PART_BYTES:
            flush()
        batch.append((rel, h))
        size += s
    if batch:
        flush()
    # The commit goes even with nothing new: it is this job's check-in.
    body = {"job": job}
    if keep_hours:
        body["keep_hours"] = keep_hours                 # the store lets go of this tree's dated files sooner
    B.post(base, key_path, "tree", tree, run, "commit", part, json.dumps(body).encode())
    for rel, h in files:
        sent[rel] = h


def mrrl_static(code_dir, log=print):
    """The radars published only as pictures (the UK composite, Taiwan, Israel, India, Portugal, Cuba, Bermuda, the
    Dominican Republic, Jamaica), as plain files the site's store can hand out: for each radar, mrrl/<site>/list.json
    (its volumes, oldest first, as the parsing server's /mrrl/list gave them) and each volume as <name>.bin.gz (the
    Archive II bytes /mrrl/vol gave, gzipped). The parsing server made these on request; it is retired, so they are
    made here from the frames the feeds job just wrote, with the server's own code (radar_extra). Only volumes not
    made before are made; ones whose frame has gone are deleted. Returns the folder."""
    import gzip
    if code_dir not in sys.path:
        sys.path.insert(0, code_dir)
    import radar_extra as X
    root = os.path.join(os.path.expanduser("~/wxdata"), "mrrl")
    os.makedirs(root, exist_ok=True)
    groups = [getattr(X, g, {}) for g in ("UK", "TW", "IL", "IN", "PT", "CUBA", "BDA", "DR", "JM")]
    made = sites = 0
    for g in groups:
        for site in sorted(g):
            if site in getattr(X, "L2DIR", {}):
                continue                                # a real Level 2 feed: the page reads it itself
            try:
                lst = X.listing(site, None)
            except Exception as e:                      # noqa: BLE001
                log(f"mrrl: {site}: no list ({e.__class__.__name__})")
                continue
            d = os.path.join(root, site)
            os.makedirs(d, exist_ok=True)
            keep = set()
            for v in lst:
                name = str(v.get("name") or "")
                if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_\-]{0,80}", name):
                    continue
                f = name + ".bin.gz"
                keep.add(f)
                p = os.path.join(d, f)
                if os.path.exists(p):
                    continue
                try:
                    body = X.fetch_volume(site, name, None)
                except Exception as e:                  # noqa: BLE001
                    log(f"mrrl: {site} {name}: {e.__class__.__name__}")
                    keep.discard(f)
                    continue
                with open(p + ".tmp", "wb") as fh:
                    fh.write(gzip.compress(body, 6))
                os.replace(p + ".tmp", p)
                made += 1
            for f in os.listdir(d):
                if f.endswith(".bin.gz") and f not in keep:
                    os.unlink(os.path.join(d, f))
            rows = [{"name": v["name"], "size": 0, "t": v.get("t")} for v in lst if (str(v.get("name")) + ".bin.gz") in keep]
            with open(os.path.join(d, "list.json.tmp"), "w") as fh:
                json.dump({"volumes": rows, "made": int(time.time())}, fh, separators=(",", ":"))
            os.replace(os.path.join(d, "list.json.tmp"), os.path.join(d, "list.json"))
            sites += 1
    log(f"mrrl: {sites} picture radars listed, {made} new volumes")
    return root


def run_job(name, base, key_path, code_dir, state, dry):
    tree, prog, args, want_seed = JOBS[name][:4]
    also = JOBS[name][4] if len(JOBS[name]) > 4 else []
    root = os.path.join(os.path.expanduser("~/wxdata"), tree)
    os.makedirs(root, exist_ok=True)
    sent = state.setdefault(tree, {})
    # Seeded from where the site reads (the store, once it is set up), not the retired parsing server: files seeded are
    # counted as sent, and seeding from the server meant the store never got them (feeds/buoys.json and every radar
    # list were missing there, Nov 25, 2026). The store lists no folders, so feeds seeds nothing and sends everything.
    seed_base = (B.STORE or base) if name == "feeds" else base
    if want_seed and not state.get("seeded-" + tree) and seed_base:
        base_for_seed = seed_base
        n = seed_top(base_for_seed, tree, root, sent)
        state["seeded-" + tree] = True
        if name == "nhccones":
            n += seed_cones(base_for_seed, root, sent)
        if name == "sst":
            n += seed_sst(root, sent)
        if name == "feeds":
            n += seed_feeds(base_for_seed, root, sent)
        log(f"{name}: started from {n} of the server's files")
    if name in ONCE and base and _get(f"{base}/{tree}/{ONCE[name]}"):
        log(f"{name}: the server has it, nothing to build")
        if not dry:
            upload(base, key_path, tree, root, name, [], sent)
        return True
    t0 = time.time()
    env = dict(os.environ, GWCFC_NO_UPLOADS="1", PYTHONPATH=code_dir, PIP_NO_INDEX="1", **ENV.get(name, {}))
    try:
        r = subprocess.run([sys.executable, os.path.join(code_dir, prog)] + args, cwd=code_dir, env=env,
                           capture_output=True, text=True, timeout=2400 if name == "sst" else 1500)
        tail = (r.stdout or "").strip().splitlines()[-1:] or (r.stderr or "").strip().splitlines()[-1:]
        note = (tail[0][:160] if tail else "") if "http" not in (tail[0].lower() if tail else "") else ""
        ran = r.returncode == 0
    except subprocess.TimeoutExpired:
        ran, note = False, "stopped: it ran out of time"
    files = changed(root, sent)
    log(f"{name}: {'ran' if ran else 'FAILED'} in {time.time() - t0:.0f}s, {len(files)} new or changed files. {note}")
    if dry:
        for rel, h in files:
            sent[rel] = h
        for other in also:
            oroot = os.path.join(os.path.expanduser("~/wxdata"), other)
            n_o = len(changed(oroot, state.setdefault(other, {}))) if os.path.isdir(oroot) else 0
            log(f"{name}: and {n_o} files of {other} (not sent)")
        return ran
    # A job that failed and wrote nothing does not check in, so the server's own copy takes over if it goes on.
    if ran or files:
        upload(base, key_path, tree, root, name, files, sent)
    # The picture radars as plain files for the store (see mrrl_static), right after the feeds that make them.
    if name == "feeds":
        try:
            mroot = mrrl_static(code_dir, log)
            msent = state.setdefault("mrrl", {})
            mfiles = changed(mroot, msent)
            if mfiles:
                upload(base, key_path, "mrrl", mroot, name, mfiles, msent, keep_hours=6)
                log(f"feeds: and {len(mfiles)} files of mrrl")
        except Exception as e:                          # noqa: BLE001
            log(f"mrrl: {e.__class__.__name__}: {str(e)[:120]}")
    # The other folders this job writes into: sent the same way (their check-in is this job's too).
    for other in also:
        oroot = os.path.join(os.path.expanduser("~/wxdata"), other)
        osent = state.setdefault(other, {})
        ofiles = changed(oroot, osent) if os.path.isdir(oroot) else []
        if ofiles:
            upload(base, key_path, other, oroot, name, ofiles, osent)
            log(f"{name}: and {len(ofiles)} files of {other}")
    return ran


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--group", required=True, choices=sorted(GROUPS))
    ap.add_argument("--loop", type=int, default=0, help="minutes to keep going (the quick group)")
    ap.add_argument("--dry", action="store_true")
    a = ap.parse_args(argv)
    base = (os.environ.get("GWCFC_BASE") or "").strip().rstrip("/")
    key = os.environ.get("GWCFC_UPLOAD_KEY") or ""
    if (not a.dry and (not base or "PRIVATE KEY" not in key)) or not B.unlock_pipeline():
        print("::warning::Nothing run: the repository secrets GWCFC_BASE and GWCFC_UPLOAD_KEY are needed.", flush=True)
        return 0
    code_dir = sys.path[0]
    key_path = None
    if not a.dry:
        fd, key_path = tempfile.mkstemp(prefix="k-")
        with os.fdopen(fd, "w") as fh:
            fh.write(key.strip() + "\n")
        os.chmod(key_path, 0o600)
    state, due = {}, {}
    end = time.time() + a.loop * 60
    while True:
        for name, every in GROUPS[a.group]:
            if time.time() >= due.get(name, 0):
                due[name] = time.time() + every
                try:
                    run_job(name, base, key_path, code_dir, state, a.dry)
                except Exception as e:                  # noqa: BLE001
                    log(f"{name}: {e.__class__.__name__}")
        if not a.loop or time.time() + 20 > end:
            break
        time.sleep(max(5, min(due.values()) - time.time()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
