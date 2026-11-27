#!/usr/bin/env python3
"""Builds the models the parsing server only ever drew on request, ahead of time, and sends them to Hugging Face.

    python3 side.py --group seasonal      # CanSIPS and CFS Monthly, every region
    python3 side.py --group mpas          # NSSL's MPAS-HTPO, MPAS-RN and MPAS-RN3 (pictures from cams.nssl.noaa.gov)
    python3 side.py --group gwcfm         # the Guta Forecasting Center's own model
    python3 side.py --group moving        # HAFS-A, HAFS-B and the RRFS fire nest, wherever today's storms and fires are
    python3 side.py --group mpas --dry    # build, upload nothing

The drawing is the server's own code (pipeline.bin, unlocked the way build.py does it). Each module's manifest and
build_png are the very functions the server called when a page asked for a picture; here every picture of the
newest run is asked for once, the run is turned to WebP like any other, and hf_store sends it. A run Hugging Face
already has whole is skipped.
"""
import argparse
import json
import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import build as B


def log(msg):
    print(time.strftime("%H:%M:%S", time.gmtime()), msg, flush=True)


def finish(run_dir, model, region, run, dry):
    """Turn the run to WebP and send it, or say why not."""
    pics = [n for n in os.listdir(run_dir) if n.endswith(".png")]
    if not pics or not os.path.exists(os.path.join(run_dir, "manifest.json")):
        log(f"{model}/{region} {run}: nothing drawn")
        return False
    if dry:
        log(f"{model}/{region} {run}: {len(pics)} pictures (dry run, not sent)")
        return True
    B.to_webp(run_dir)
    import hf_store
    t = time.time()
    hf_store.send_run(run_dir, model, region, run, log=log)
    log(f"{model}/{region} {run}: {len(pics)} pictures sent in {time.time() - t:.0f}s")
    return True


def have(model, region, run, dry):
    if dry:
        return False
    try:
        import hf_store
        return hf_store.enabled() and hf_store.has(model, region, run)
    except Exception:                          # noqa: BLE001
        return False


def draw_all(jobs, workers):
    """jobs: [(callable, args)]; failures are counted, never fatal."""
    bad = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for ok in ex.map(lambda j: _safe(*j), jobs):
            bad += 0 if ok else 1
    return bad


def _safe(fn, args):
    try:
        r = fn(*args)
        return r is not False
    except Exception:                          # noqa: BLE001
        return False


def seasonal(out, dry):
    import seasonal_outlooks as S
    for model in S.MODELS:
        rs = S.runs(model)
        if not rs:
            log(f"{model}: no runs found")
            continue
        run = rs[0]
        for region in S.REGIONS:
            if have(model, region, run, dry):
                log(f"{model}/{region} {run}: already on Hugging Face")
                continue
            d = os.path.join(out, model, region, run)
            os.makedirs(d, exist_ok=True)
            man = S.manifest(model, region, run)
            jobs = [(S.build_png, (model, region, run, f, h, os.path.join(d, f"{f}_f{h:03d}.png")))
                    for f, spec in man["fields"].items() for h in spec["hours"]]
            bad = draw_all(jobs, 4)
            log(f"{model}/{region} {run}: {len(jobs) - bad}/{len(jobs)} pictures")
            with open(os.path.join(d, "manifest.json"), "w") as fh:
                json.dump(man, fh, separators=(",", ":"))
            finish(d, model, region, run, dry)


def mpas(out, dry):
    import nssl_cams as N
    for model in N.MODELS:
        rs = N.runs_of(model)
        if not rs:
            log(f"{model}: no runs at NSSL right now")
            continue
        run, region = rs[0], "conus"
        if time.time() - time.mktime(time.strptime(run[:8], "%Y%m%d")) > 7 * 86400:
            log(f"{model}: newest run {run} is over a week old (NSSL has stopped running it); not shown")
            continue
        if have(model, region, run, dry):
            log(f"{model}/{region} {run}: already on Hugging Face")
            continue
        d = os.path.join(out, model, region, run)
        os.makedirs(d, exist_ok=True)
        man = N.manifest(model, run)
        jobs = []
        for f, spec in man["fields"].items():
            jobs.append((N.build_png, (model, run, f, None, os.path.join(d, f"{f}_legend.png"))))
            jobs += [(N.build_png, (model, run, f, h, os.path.join(d, f"{f}_f{h:03d}.png"))) for h in spec["hours"]]
        # NSSL's own web server: a few at a time, not a burst.
        bad = draw_all(jobs, 3)
        log(f"{model} {run}: {len(jobs) - bad}/{len(jobs)} pictures")
        # Only the hours and products that came through are listed, so the page never asks for a missing picture.
        for f, spec in list(man["fields"].items()):
            spec["hours"] = [h for h in spec["hours"] if os.path.exists(os.path.join(d, f"{f}_f{h:03d}.png"))]
            if not spec["hours"]:
                del man["fields"][f]
        man["hours"] = sorted({h for s in man["fields"].values() for h in s["hours"]})
        with open(os.path.join(d, "manifest.json"), "w") as fh:
            json.dump(man, fh, separators=(",", ":"))
        finish(d, model, region, run, dry)


def _seed_from_hf(models_regions):
    """The newest run of each (model, region) from Hugging Face, put on this machine's disk the way the server kept
    it: flat in models/<model>/<region>/<run>/, the .npz data only (what GWCFM reads), and a manifest that names the
    pictures .png again. GWCFM blends these members' runs off the disk, which on a fresh builder starts empty."""
    import gfs_pipeline as G
    import hf_store
    lst = (hf_store.read_list() or {}).get("models") or {}
    want = []
    for m, r in models_regions:
        e = ((lst.get(m) or {}).get("regions") or {}).get(r)
        if not e or not e.get("run"):
            continue
        run = e["run"]
        want.append((m, r, run))
    if not want:
        return 0
    # Only each run's own folder is listed (the whole dataset holds hundreds of thousands of files), and the
    # files are fetched straight from their addresses, many at a time.
    import requests
    from huggingface_hub import HfApi
    api, t, n = HfApi(), time.time(), 0
    jobs = []
    for m, r, run in want:
        base = f"models/{m}/{r}/{run}"
        dst = os.path.join(G.OUT_DIR, m, r, run)
        os.makedirs(dst, exist_ok=True)
        try:
            files = [x.path for x in api.list_repo_tree(hf_store.REPO, repo_type="dataset", path_in_repo=base, recursive=True)
                     if getattr(x, "size", None) is not None]
        except Exception as e:                 # noqa: BLE001
            log(f"gwcfm: {base} could not be listed ({e.__class__.__name__})")
            continue
        # The data files where the run has them; otherwise its pictures, which GWCFM reads back to numbers by colour.
        have_npz = {os.path.basename(f)[:-4] for f in files if f.endswith(".npz")}
        for f in files:
            b = os.path.basename(f)
            if f.endswith(".npz") or f.endswith("/manifest.json") or (f.endswith(".webp") and b[:-5] not in have_npz):
                jobs.append((f, os.path.join(dst, b)))

    def get(job):
        url, path = f"{hf_store.READ}/{job[0]}", job[1]
        for _ in range(3):
            try:
                r = requests.get(url, timeout=60)
                if r.status_code == 200:
                    if path.endswith(".webp"):
                        from PIL import Image
                        import io
                        with Image.open(io.BytesIO(r.content)) as im:
                            im.save(path[:-5] + ".png", "PNG", compress_level=1)
                    else:
                        with open(path, "wb") as fh:
                            fh.write(r.content)
                    return True
            except Exception:                  # noqa: BLE001
                time.sleep(2)
        return False
    with ThreadPoolExecutor(max_workers=16) as ex:
        n = sum(ex.map(get, jobs))
    # The manifests name the pictures .webp (hf_store turned them); the server's code reads .png names.
    for m, r, run in want:
        mp = os.path.join(G.OUT_DIR, m, r, run, "manifest.json")
        if not os.path.exists(mp):
            continue
        with open(mp) as fh:
            man = json.load(fh)
        def back(o):
            if isinstance(o, dict):
                return {k: back(v) for k, v in o.items() if k != "ext"}
            if isinstance(o, list):
                return [back(v) for v in o]
            return o[:-5] + ".png" if isinstance(o, str) and o.endswith(".webp") else o
        with open(mp, "w") as fh:
            json.dump(back(man), fh)
    log(f"gwcfm: {n} data files of {len(want)} member runs fetched from Hugging Face in {time.time() - t:.0f}s")
    return n


def gwcfm(out, dry):
    import gfs_pipeline as G
    import gwcfm as W
    need = set()
    for region, d in W.DOMAINS.items():
        for m in tuple(d["base"]) + tuple(d["members"]) + ((d["truth"],) if d.get("truth") else ()):
            need.add((m, region))
        for m, r in (d.get("extra") or {}).items():
            need.add((m, r))
    try:
        _seed_from_hf(sorted(need))
    except Exception as e:                     # noqa: BLE001
        log(f"gwcfm: its member runs could not be fetched ({e.__class__.__name__}: {e})")
    man = W.build_latest()
    if not man:
        log("gwcfm: nothing built (its newest run is done, or its inputs are not out yet)")
        return
    run = man.get("run")
    root = os.path.join(G.OUT_DIR, "gwcfm")
    for region in sorted(os.listdir(root)) if os.path.isdir(root) else []:
        d = os.path.join(root, region, run)
        if os.path.isdir(d) and not have("gwcfm", region, run, dry):
            finish(d, "gwcfm", region, run, dry)


def moving(out, dry):
    """The models whose areas change every day: HAFS-A and HAFS-B (one parent and one storm box per active storm,
    none when the tropics are quiet) and the RRFS fire weather nest (where the fires are). Their areas are asked of
    the drawing code, then each is built by build.py exactly as a scheduled model is."""
    import gfs_pipeline as G
    for name in ("hafs", "hafsb", "rrfsfire"):
        m = G.MODELS.get(name)
        if not m:
            continue
        try:
            regs = list(G.regions_of(m))
        except Exception as e:                 # noqa: BLE001
            log(f"{name}: its areas could not be worked out ({e.__class__.__name__})")
            continue
        if not regs:
            log(f"{name}: nothing to build today (no active storm or fire domain)")
            continue
        for region in regs:
            try:
                B.main(["--model", name, "--region", region] + (["--dry"] if dry else []))
            except SystemExit:
                pass
            except Exception as e:             # noqa: BLE001
                log(f"{name}/{region}: {e.__class__.__name__}: {e}")


GROUPS = {"seasonal": seasonal, "mpas": mpas, "gwcfm": gwcfm, "moving": moving}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--group", required=True, choices=sorted(GROUPS))
    ap.add_argument("--dry", action="store_true")
    a = ap.parse_args(argv)
    key = os.environ.get("GWCFC_UPLOAD_KEY") or ""
    if not a.dry and "PRIVATE KEY" not in key:
        print("::warning::Nothing built: add the repository secret GWCFC_UPLOAD_KEY.", flush=True)
        return 0
    if not B.unlock_pipeline():
        print("::warning::Nothing built: the drawing code could not be unlocked.", flush=True)
        return 0
    import gfs_pipeline as G
    out = os.path.join(G.OUT_DIR, "_side")
    os.makedirs(out, exist_ok=True)
    try:
        GROUPS[a.group](out, a.dry)
    finally:
        shutil.rmtree(out, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
