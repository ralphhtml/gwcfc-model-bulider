#!/usr/bin/env python3
"""Runs the parsing server's cyclone and ensemble jobs here and uploads what they wrote, signed.

    python3 tree.py --job cyclone     # GEFS centres, ECMWF cyclone odds, GEPS and AIFS tracks, GFS and ECMWF lows
    python3 tree.py --job cyclones    # Google DeepMind's cyclone tracks and genesis grids
    python3 tree.py --job ens-gefs    # every member's fields of one ensemble (gefs, geps, sref, gfs, ecmwf, ecmwfens)
    python3 tree.py --job soundings   # the hourly sounding picture of every upper-air site
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
    "soundings": ("soundings", ["manifest.json", "state.json"]),
    # Not a folder of data: the ECMWF weeklies are model runs, uploaded as such; this is only where the job checks in.
    "weeklies": ("checkins", []),
}
ENS_MODELS = ["gefs", "geps", "sref", "gfs", "ecmwf", "ecmwfens"]
for _m in ENS_MODELS:
    JOBS["ens-" + _m] = ("ens", ["index.json"])
LAST = ("latest.json", "manifest.json", "index.json")


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
    return sorted(out, key=lambda r: (r.rsplit("/", 1)[-1] in LAST, r.count("/") == 0, r))


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


def _json(path):
    try:
        with open(path) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


def run_ens(base, root, seeded, model):
    """One ensemble's member fields. Returns what the commit should carry: the model's entry for the server's index
    (the server puts it in beside the other models' entries) and the one run to keep."""
    import ens_fields_pipeline as ef
    ens = ef.ENSEMBLES[model]
    d, c = ef.newest_cycle(ens)
    run = f"{d}_{c}"
    entry = (_json(os.path.join(root, "index.json")).get("models") or {}).get(model) or {}
    if entry.get("run") == run and entry.get("complete"):
        log(f"{model}: the server has {run} whole")
        return {}
    if entry.get("run") == run and entry.get("hours"):
        # Part built already (an earlier job here was cut short): carry on from its last hour, as the pipeline does
        # from a run on its own disk. It needs the run's manifest and, for the running rain total, the last hour's.
        rels = [f"{model}/{run}/manifest.json"]
        if "qpf" in (entry.get("fields") or []):
            rels.append(f"{model}/{run}/qpf_f{int(entry['hours'][-1]):03d}.bin.gz")
        for rel in rels:
            body = fetch(base, f"ens/{rel}")
            if body is None:
                continue
            path = os.path.join(root, rel)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as fh:
                fh.write(body)
            seeded[rel] = hashlib.sha256(body).hexdigest()
    man = ef.build(model, d, c)
    if not man.get("hours"):
        log(f"{model}: {run} is not published yet")
        seeded[f"{model}/{run}/manifest.json"] = sha(os.path.join(root, model, run, "manifest.json"))
        return {}
    mine = (_json(os.path.join(root, "index.json")).get("models") or {}).get(model)
    return {"index_entry": {"model": model, "entry": mine}, "only": {model: [run]}} if mine else {}


def run_soundings(base, root, seeded):
    """The sites the server does not have this hour's sounding of, then one manifest: the server's frames of the
    last day and the new ones."""
    os.environ.setdefault("GWCFC_SND_PASS_MAX", "500")
    os.environ.setdefault("GWCFC_SND_PASS_SECS", "6000")
    import datetime as dt
    import sounding_pipeline as sp
    theirs = (_json(os.path.join(root, "manifest.json")).get("sites") or {})
    now = dt.datetime.now(dt.timezone.utc)
    stamp = sp.wanted_valid(now).strftime("%Y%m%d_%H%M%S")
    need = [s[0] for s in sp.SITES if stamp not in ((theirs.get(s[0]) or {}).get("frames") or [])]
    if not need:
        log(f"soundings: the server has {stamp} of every site")
        return {"keep_hours": sp.KEEP_HOURS}
    gone = sp.missing_deps()
    if gone:
        log("soundings: cannot build: " + "; ".join(f"{m}: {e}" for m, _w, e in gone)[:600])
        return {"no_commit": True}
    log(f"soundings: {len(need)} of {len(sp.SITES)} sites to build for {stamp}")
    sp.run_pass(only=need, now=now)
    cut = (now - dt.timedelta(hours=sp.KEEP_HOURS)).strftime("%Y%m%d_%H%M%S")
    man = _json(os.path.join(root, "manifest.json"))
    sites = man.get("sites") or {}
    for sid, name, lat, lon in sp.SITES:
        old = [f for f in ((theirs.get(sid) or {}).get("frames") or []) if isinstance(f, str) and f >= cut]
        new = (sites.get(sid) or {}).get("frames") or []
        frames = sorted(set(old) | set(new))
        if not frames:
            sites.pop(sid, None)
            continue
        sites[sid] = {"name": name, "lat": lat, "lon": lon, "dir": f"{sid}/{frames[-1]}",
                      "valid": dt.datetime.strptime(frames[-1], "%Y%m%d_%H%M%S").strftime("%Y-%m-%dT%H:%M:%SZ"),
                      "frames": frames}
    man.update({"updated": now.isoformat(), "source": "rap",
                "engine": {"fetch": "SounderPy", "params": "SHARPpy"}, "sites": sites})
    with open(os.path.join(root, "manifest.json"), "w") as fh:
        json.dump(man, fh)
    built = sum(1 for sid in need if os.path.exists(os.path.join(root, sid, stamp, "skewt.png")))
    log(f"soundings: {built} of {len(need)} built")
    # With nothing built the upstream is down: no check-in, so the server's own job is not kept waiting on us.
    return {"keep_hours": sp.KEEP_HOURS} if built else {"no_commit": True}


def run_weeklies(base, key):
    """The ECMWF weeklies and seasonal: each is a model run of its own, uploaded the way build.py uploads one.
    Returns False when nothing could be asked of ECMWF at all (then there is no check-in)."""
    import gfs_pipeline as G
    import ecmwf_weeklies as W
    import requests
    todo = [(W.NAME, W.OUT, W.build), (W.SEAS_NAME, W.SEAS_OUT, W.build_seasonal)]
    # What the server has: its newest run's manifest, put where the job looks, so a run it has whole is not drawn
    # again. Only the manifest: a run it has in part is drawn afresh.
    theirs = {}
    for name, out, _fn in todo:
        try:
            idx = requests.get(f"{base}/models/{name}/index.json", timeout=60).json()["regions"][W.REGION]
            man = requests.get(f"{base}/models/{name}/{W.REGION}/{idx['run']}/manifest.json", timeout=60).json()
            theirs[name] = (idx["run"], len(man.get("fields") or {}), bool(man.get("partial")))
        except Exception:                               # noqa: BLE001
            theirs[name] = ("", 0, True)
    fd, key_path = tempfile.mkstemp(prefix="k-")
    with os.fdopen(fd, "w") as fh:
        fh.write(key.strip() + "\n")
    os.chmod(key_path, 0o600)
    asked = False
    for name, out, fn in todo:
        t_run, t_fields, t_part = theirs[name]
        try:
            newest = (W.newest_run(W.FIELDS["wkt2a"]["product"]) if name == W.NAME
                      else W.seasonal_run(next(iter(W.SEAS_FIELDS.values()))["product"]))[0]
        except Exception as e:                          # noqa: BLE001
            log(f"{name}: could not ask ECMWF for its newest run: {e.__class__.__name__}")
            continue
        if not newest:
            log(f"{name}: ECMWF has no run drawn right now")
            asked = True
            continue
        asked = True
        run = newest.strftime("%Y%m%d_00")
        if run == t_run and not t_part and t_fields:
            log(f"{name}: the server has {run} whole")
            continue
        try:
            fn()
        except Exception as e:                          # noqa: BLE001
            log(f"{name}: {e.__class__.__name__}: {str(e)[:200]}")
            continue
        run_dir = os.path.join(out, run)
        try:
            with open(os.path.join(run_dir, "manifest.json")) as fh:
                man = json.load(fh)
        except (OSError, ValueError):
            log(f"{name}: nothing was built")
            continue
        if man.get("partial") or (run == t_run and len(man.get("fields") or {}) < t_fields):
            log(f"{name}: {run} came out with {len(man.get('fields') or {})} products, fewer than the server's; kept back")
            continue
        files = [n for n in os.listdir(run_dir) if not n.startswith(".") and os.path.isfile(os.path.join(run_dir, n))]
        n_parts = 0
        for i, names in enumerate(B.parts(run_dir)):
            B.post(base, key_path, name, W.REGION, run, "part", i, B.tar_of(run_dir, names))
            n_parts += 1
        got = B.post(base, key_path, name, W.REGION, run, "commit", n_parts, json.dumps({"files": len(files)}).encode())
        log(f"{name}: uploaded {run}, {got.get('files')} files")
    os.unlink(key_path)
    return asked


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
    extra = {}
    if a.job.startswith("ens-"):
        extra = run_ens(base, root, seeded, a.job[4:]) or {}
    elif a.job == "soundings":
        extra = run_soundings(base, root, seeded) or {}
    elif a.job == "weeklies":
        extra = {} if (a.dry or run_weeklies(base, key)) else {"no_commit": True}
    else:
        (run_cyclone if a.job == "cyclone" else run_cyclones)(base, root, seeded)
    rels_out = changed(root, seeded)
    if tree == "ens":
        # The server puts this model's entry into its index itself (see the commit), so the other models' jobs,
        # running at the same time, do not overwrite each other's.
        mine = a.job[4:] + "/"
        rels_out = [r for r in rels_out if r.startswith(mine)]
    if extra.pop("no_commit", False):
        rels_out = []
        a.dry = True
    size = sum(os.path.getsize(os.path.join(root, r)) for r in rels_out)
    log(f"{a.job}: {len(rels_out)} new or changed files, {size / 1e6:.1f} MB, in {time.time() - t0:.0f}s")
    if not rels_out:
        log("nothing new")
    if a.dry:
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
    out = B.post(base, key_path, "tree", tree, run, "commit", part, json.dumps(dict(extra, job=a.job)).encode())
    # The commit goes even with nothing new: it is the check-in the server's own job reads to know this one is alive.
    log(f"uploaded {len(rels_out)} files in {part} parts; the server let go of {out.get('pruned', 0)} old ones")
    os.unlink(key_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
