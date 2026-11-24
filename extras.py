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
}
GROUPS = {
    "quick": [("glm", 120), ("tropx", 600), ("stormsat", 600)],
    "slow":  [("wafflehouse", 0), ("spaghetti", 0), ("satwinds", 0), ("wind", 0), ("ace", 0)],
}
OK_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.\-]{0,120}\.(png|json|webp|npz|csv|geojson)$")
OK_DIR = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_\-]{0,60}$")
PART_BYTES = 20 * 1024 * 1024


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


def upload(base, key_path, tree, root, job, files, sent):
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
    B.post(base, key_path, "tree", tree, run, "commit", part, json.dumps({"job": job}).encode())
    for rel, h in files:
        sent[rel] = h


def run_job(name, base, key_path, code_dir, state, dry):
    tree, prog, args, want_seed = JOBS[name][:4]
    also = JOBS[name][4] if len(JOBS[name]) > 4 else []
    root = os.path.join(os.path.expanduser("~/wxdata"), tree)
    os.makedirs(root, exist_ok=True)
    sent = state.setdefault(tree, {})
    if want_seed and not state.get("seeded-" + tree) and base:
        n = seed_top(base, tree, root, sent)
        state["seeded-" + tree] = True
        log(f"{name}: started from {n} of the server's files")
    t0 = time.time()
    env = dict(os.environ, GWCFC_NO_UPLOADS="1", PYTHONPATH=code_dir, PIP_NO_INDEX="1")
    try:
        r = subprocess.run([sys.executable, os.path.join(code_dir, prog)] + args, cwd=code_dir, env=env,
                           capture_output=True, text=True, timeout=1500)
        tail = (r.stdout or "").strip().splitlines()[-1:] or (r.stderr or "").strip().splitlines()[-1:]
        note = (tail[0][:160] if tail else "") if "http" not in (tail[0].lower() if tail else "") else ""
        ran = r.returncode == 0
    except subprocess.TimeoutExpired:
        ran, note = False, "stopped after 25 minutes"
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
