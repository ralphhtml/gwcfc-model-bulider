#!/usr/bin/env python3
"""Runs the parsing server's cyclone and ensemble jobs here and uploads what they wrote, signed.

    python3 tree.py --job cyclone     # GEFS centres, ECMWF cyclone odds, GEPS and AIFS tracks, GFS and ECMWF lows
    python3 tree.py --job cyclones    # Google DeepMind's cyclone tracks and genesis grids
    python3 tree.py --job cyclone --dry

These jobs keep their own small index files (which run is the newest, which models have lows), so the server's
current ones are fetched first: a job then skips what the server already has, and adds to an index instead of
replacing it. Only files that are new or changed are uploaded, the index files last.

The server's address and the signing key come from the environment (repository secrets), as in build.py.
"""
import argparse
import hashlib
import io
import json
import os
import sys
import tarfile
import tempfile
import time

import build as B

PART_BYTES = 40 * 1024 * 1024
JOBS = {
    # job: (folder under the data directory, files of the server's to start from)
    "cyclone": ("enscenters", ["latest.json", "lows/latest.json", "ecmwf/latest.json", "geps/latest.json",
                               "aifs-ens/latest.json"]),
    "cyclones": ("cyclones", ["latest.json"]),
}


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch(base, rel):
    import requests
    try:
        r = requests.get(f"{base}/{rel}", timeout=60, headers={"User-Agent": "gwcfc-model-builder"})
        return r.content if r.status_code == 200 else None
    except Exception:                                   # noqa: BLE001
        return None


def seed(base, tree, root, rels):
    """The server's index files, put where the jobs will read them. Returns {relative path: sha} of what was put."""
    got = {}
    for rel in rels:
        body = fetch(base, f"{tree}/{rel}") if base else None
        if body is None:
            continue
        try:
            json.loads(body)
        except ValueError:
            continue
        path = os.path.join(root, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as fh:
            fh.write(body)
        got[rel] = hashlib.sha256(body).hexdigest()
    return got


def changed(root, seeded):
    """Every file under root that is new or differs from what was fetched, index files (latest.json) last."""
    out = []
    for dp, dn, fn in os.walk(root):
        dn[:] = [d for d in dn if not d.startswith(".")]
        for n in fn:
            if n.startswith(".") or ".tmp" in n or n == "progress.json":
                continue
            p = os.path.join(dp, n)
            rel = os.path.relpath(p, root).replace(os.sep, "/")
            if rel in seeded and seeded[rel] == sha(p):
                continue
            out.append(rel)
    return sorted(out, key=lambda r: (r.endswith("latest.json"), r.count("/") == 0, r))


def run_cyclone(base, root, seeded):
    import enscenters_pipeline as ens
    import ecmwf_tc_pipeline as etc
    import ens_more_pipeline as em
    import lows_pipeline as lp
    import requests

    def have(rel):
        try:
            with open(os.path.join(root, rel)) as fh:
                return json.load(fh)
        except (OSError, ValueError):
            return {}

    # GEFS centres: the pipeline builds whatever it is given, so the "already there" question is asked here.
    d, c = ens.newest_cycle()
    cur = have("latest.json")
    if cur.get("run") == f"{d}_{c}" and int(cur.get("members") or 0) >= len(ens.MEMBERS) - 1:
        log(f"gefs: the server has {d}_{c} whole")
    elif not ens.check(d, c, ens.DEFAULT_STEP):
        log(f"gefs: {d} {c}z is not published yet")
    else:
        ens.build(d, c, ens.DEFAULT_STEP, ens.DEFAULT_OUT, ens.MEMBERS, verbose=False)

    # ECMWF cyclone odds: skips by itself when the fetched latest.json names the newest run.
    try:
        etc.main([])
    except Exception as e:                              # noqa: BLE001
        log(f"ecmwf-tc: {e.__class__.__name__}: {str(e)[:200]}")

    # GEPS and AIFS tracks.
    session = requests.Session()
    for model in sorted(em.MODELS):
        try:
            cfg = em.MODELS[model]
            md, mc = em.newest_published(model, session)
            if not md:
                log(f"{model}: no run published yet")
                continue
            if have(f"{model}/latest.json").get("run") == f"{md}_{mc}":
                log(f"{model}: the server has {md}_{mc}")
                continue
            em.build(model, md, mc, cfg["step_h"], cfg["out_h"], cfg["members"], session)
        except Exception as e:                          # noqa: BLE001
            log(f"{model}: {e.__class__.__name__}: {str(e)[:200]}")

    # GFS and ECMWF lows: skips by itself from the fetched lows/latest.json.
    try:
        lp.main([])
    except Exception as e:                              # noqa: BLE001
        log(f"lows: {e.__class__.__name__}: {str(e)[:200]}")


def run_cyclones(base, root, seeded):
    import cyclones_pipeline as cp
    # The newest run's manifest too: that is what the pipeline looks for to say "already built".
    try:
        with open(os.path.join(root, "latest.json")) as fh:
            rel = json.load(fh).get("path")
    except (OSError, ValueError):
        rel = None
    if rel:
        seeded.update(seed(base, "cyclones", root, [rel]))
    try:
        cp.main([])
    except Exception as e:                              # noqa: BLE001
        log(f"cyclones: {e.__class__.__name__}: {str(e)[:200]}")


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--job", required=True, choices=sorted(JOBS))
    ap.add_argument("--dry", action="store_true", help="run, do not upload")
    a = ap.parse_args(argv)
    base = (os.environ.get("GWCFC_BASE") or "").strip().rstrip("/")
    key = os.environ.get("GWCFC_UPLOAD_KEY") or ""
    if not a.dry and (not base or "PRIVATE KEY" not in key):
        print("::warning::Nothing built: add the repository secrets GWCFC_BASE and GWCFC_UPLOAD_KEY.", flush=True)
        return 0
    if not B.unlock_pipeline():
        print("::warning::Nothing built: the code could not be unlocked (is GWCFC_UPLOAD_KEY the right key?)", flush=True)
        return 0
    tree, rels = JOBS[a.job]
    root = os.path.join(os.path.expanduser("~/wxdata"), tree)
    os.makedirs(root, exist_ok=True)
    seeded = seed(base, tree, root, rels)
    log(f"{a.job}: started from {len(seeded)} of the server's index files")
    t0 = time.time()
    (run_cyclone if a.job == "cyclone" else run_cyclones)(base, root, seeded)
    rels_out = changed(root, seeded)
    size = sum(os.path.getsize(os.path.join(root, r)) for r in rels_out)
    log(f"{a.job}: {len(rels_out)} new or changed files, {size / 1e6:.1f} MB, in {time.time() - t0:.0f}s")
    if a.dry or not rels_out:
        if not rels_out:
            log("nothing new: the server has everything")
        return 0

    fd, key_path = tempfile.mkstemp(prefix="k-")
    with os.fdopen(fd, "w") as fh:
        fh.write(key.strip() + "\n")
    os.chmod(key_path, 0o600)
    run = time.strftime("%Y%m%d_%H", time.gmtime())
    batch, bsize, part = [], 0, 0

    def send(names):
        nonlocal part
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tf:
            for r in names:
                tf.add(os.path.join(root, r), arcname=r, recursive=False)
        B.post(base, key_path, "tree", tree, run, "part", part, buf.getvalue())
        part += 1
    for r in rels_out:
        s = os.path.getsize(os.path.join(root, r))
        if batch and bsize + s > PART_BYTES:
            send(batch)
            batch, bsize = [], 0
        batch.append(r)
        bsize += s
    if batch:
        send(batch)
    out = B.post(base, key_path, "tree", tree, run, "commit", part, json.dumps({"job": a.job}).encode())
    log(f"uploaded {len(rels_out)} files in {part} parts; the server let go of {out.get('pruned', 0)} old ones")
    os.unlink(key_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
