#!/usr/bin/env python3
"""Draws one run of one weather model and uploads it, signed, to the Guta Forecasting Center parsing server.

    python3 build.py --model hrrr --region conus            # the newest complete run, unless the server has it
    python3 build.py --model hrrr --region conus --run 20261123_18
    python3 build.py --model hrrr --region conus --hours 0,1 --dry    # a short test, nothing uploaded

The drawing is the parsing server's own code, kept here only encrypted (pipeline.bin, see unlock_pipeline), so a
run built here is the same run the server would have built and nobody reading this repository can read that code. The server's address and the signing key come from the environment
(GWCFC_BASE, GWCFC_UPLOAD_KEY: repository secrets, never printed, never written to the log).

Every request is signed with the key (Ed25519): the server keeps only the public half and takes nothing that is
not signed, fresh, and whole. See pi/model_upload.py in the main repository for the server's side.
"""
import argparse
import base64
import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))


def unlock_pipeline():
    """The drawing code, unpacked to a temporary folder and put on the import path.

    It is kept in this public repository only as pipeline.bin, encrypted (AES-256): nobody reading the repository
    can read it. The key is derived from the signing key, a repository secret, so only a build here (or the owner)
    can unlock it. A readable pipeline/ folder beside this file, if there is one (a private checkout), is used as
    it is."""
    plain = os.path.join(HERE, "pipeline")
    if os.path.isdir(plain):
        sys.path.insert(0, plain)
        return True
    key = os.environ.get("GWCFC_UPLOAD_KEY") or ""
    body = "".join(ln.strip() for ln in key.splitlines() if ln.strip() and not ln.startswith("-----"))
    if not body:
        return False
    env = dict(os.environ, GWCFC_BUNDLE_PASS=hashlib.sha256(body.encode()).hexdigest())
    out = tempfile.mkdtemp(prefix="pl-")
    dec = subprocess.run(["openssl", "enc", "-d", "-aes-256-cbc", "-pbkdf2", "-iter", "200000", "-in",
                          os.path.join(HERE, "pipeline.bin"), "-pass", "env:GWCFC_BUNDLE_PASS"],
                         capture_output=True, env=env)
    if dec.returncode != 0:
        return False
    with tarfile.open(fileobj=io.BytesIO(dec.stdout), mode="r:gz") as tf:
        tf.extractall(out, filter="data")
    sys.path.insert(0, out)
    return True


# The builder builds what it is asked for (the server-side "an upload arrived, do not build" rule is not for it),
# and draws pictures the size the server does, so a run is the size the server's disk and tunnel expect.
os.environ.setdefault("GWCFC_NO_UPLOADS", "1")
os.environ.setdefault("GWCFC_MAX_TOTAL_PX", "1600000")

PART_BYTES = 6 * 1024 * 1024    # the host takes at most 8 MiB in one request (PHP post_max_size; it ignores .user.ini): a bigger body arrives empty


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def sign(msg, key_path):
    d = tempfile.mkdtemp(prefix="sig-")
    mp, sp = os.path.join(d, "m"), os.path.join(d, "s")
    with open(mp, "wb") as fh:
        fh.write(msg)
    subprocess.run(["openssl", "pkeyutl", "-sign", "-inkey", key_path, "-rawin", "-in", mp, "-out", sp],
                   check=True, capture_output=True)
    with open(sp, "rb") as fh:
        return fh.read()


# The site's own store (store.php on the web host): every upload goes there too, so the site keeps its data when the
# parsing server is gone. Its address is public (it is the site), so it is not a secret.
STORE = (os.environ.get("GWCFC_STORE") or "https://radar.gwcfc.net/data").strip().rstrip("/")


def post(base, key_path, model, region, run, kind, part, body, tries=6):
    """Sent to the site's store (and only there once it is set up: the parsing server is retired, and it answered every
    part "the disk is too full" six times over with waits between, about three minutes a part, before the store was
    asked at all, so one HRRR run took over an hour to upload, Nov 25, 2026). Without a store, to the server."""
    bases = [STORE] if STORE else [base]
    out, errs = None, []
    for b in bases:
        try:
            r = _post_one(b, key_path, model, region, run, kind, part, body, tries if b == bases[0] else 4)
            out = out or r
        except RuntimeError as e:
            errs.append(("store" if b == STORE else "server") + ": " + str(e))
    if out is None:
        raise RuntimeError("; ".join(errs))
    for e in errs:
        print("::warning::" + e, flush=True)
    return out


def _post_one(base, key_path, model, region, run, kind, part, body, tries=6):
    import requests
    last = ""
    for n in range(tries):
        f = {"model": model, "region": region, "run": run, "kind": kind, "part": part,
             "sha256": hashlib.sha256(body).hexdigest(), "ts": int(time.time())}
        msg = "\n".join(["gwcfc-upload-v1", model, region, run, kind, str(part), f["sha256"], str(f["ts"])]).encode()
        headers = {"X-GWCFC-Model": model, "X-GWCFC-Region": region, "X-GWCFC-Run": run, "X-GWCFC-Kind": kind,
                   "X-GWCFC-Part": str(part), "X-GWCFC-Sha256": f["sha256"], "X-GWCFC-Ts": str(f["ts"]),
                   "X-GWCFC-Sig": base64.b64encode(sign(msg, key_path)).decode(),
                   "Content-Type": "application/octet-stream", "User-Agent": "gwcfc-model-builder"}
        try:
            r = requests.post(base + "/models/upload", data=body, headers=headers, timeout=300)
            if r.status_code == 200:
                return r.json()
            last = f"HTTP {r.status_code} {r.text[:200]}"
            # A body that arrived damaged (the host cut it short) is worth sending again; any other refusal is not.
            if r.status_code in (400, 403, 409, 413) and "does not match its hash" not in r.text:
                break                                   # refused: asking again will not change the answer
        except Exception as e:                          # noqa: BLE001
            last = e.__class__.__name__                 # the address is in the message, so only the kind is kept
        time.sleep(min(60, 5 * 2 ** n))
    raise RuntimeError(f"upload of {kind} {part} failed: {last}")


def parts(run_dir):
    """The run's files as tars of whole files, each under PART_BYTES (a single bigger file gets a part to itself).
    The manifest goes last, so a run is never listed before its pictures are there."""
    names = sorted(n for n in os.listdir(run_dir) if not n.startswith(".") and os.path.isfile(os.path.join(run_dir, n))
                   and n != "manifest.json")
    names.append("manifest.json")
    batch, size = [], 0
    for n in names:
        s = os.path.getsize(os.path.join(run_dir, n))
        if batch and size + s > PART_BYTES:
            yield batch
            batch, size = [], 0
        batch.append(n)
        size += s
    if batch:
        yield batch


def _webp_one(path):
    """One PNG as lossless WebP with every pixel kept exactly (exact=True keeps the colour of see-through pixels too,
    which the page reads as data). The PNG goes once the WebP is written; any failure keeps the PNG."""
    from PIL import Image
    out = path[:-4] + ".webp"
    try:
        with Image.open(path) as im:
            im.load()
            im.save(out, "WEBP", lossless=True, quality=25, method=2, exact=True)
        os.unlink(path)
        return True
    except Exception:                                   # noqa: BLE001
        try:
            os.unlink(out)
        except OSError:
            pass
        return False


def to_webp(run_dir):
    """Every picture in a run as lossless WebP instead of PNG: the same pixels in about half the room, so the site's
    store (one 20 GB hosting plan for everything) holds about twice as many runs (Nov 25, 2026). The manifest says
    so with "ext": "webp" and its file patterns, and the page asks for .webp then; a run without it is read as PNG."""
    from concurrent.futures import ProcessPoolExecutor
    pngs = [os.path.join(run_dir, n) for n in os.listdir(run_dir) if n.endswith(".png")]
    if not pngs:
        return 0, 0
    with ProcessPoolExecutor(max_workers=os.cpu_count() or 2) as ex:
        ok = sum(ex.map(_webp_one, pngs, chunksize=16))
    if ok != len(pngs):                                 # a run is all one kind: put back what was converted
        from PIL import Image
        for p in pngs:
            w = p[:-4] + ".webp"
            if not os.path.exists(p) and os.path.exists(w):
                with Image.open(w) as im:
                    im.save(p, "PNG")
                os.unlink(w)
        return 0, len(pngs)
    mp = os.path.join(run_dir, "manifest.json")
    with open(mp) as fh:
        man = json.load(fh)

    def swap(o):
        if isinstance(o, dict):
            return {k: swap(v) for k, v in o.items()}
        if isinstance(o, list):
            return [swap(v) for v in o]
        if isinstance(o, str) and o.endswith(".png"):
            return o[:-4] + ".webp"
        return o
    man = swap(man)
    man["ext"] = "webp"
    with open(mp, "w") as fh:
        json.dump(man, fh, indent=1)
    return ok, len(pngs)


def tar_of(run_dir, names):
    b = io.BytesIO()
    with tarfile.open(fileobj=b, mode="w") as tf:
        for n in names:
            tf.add(os.path.join(run_dir, n), arcname=n, recursive=False)
    return b.getvalue()


def server_has(base, model, region, run):
    """Whether this run is already whole where the site reads it (so it is not built and sent again). With the site's
    store set up, the store alone decides: the parsing server is retired (Nov 2026), and asking it too made a server
    that answers with an error count every run as missing, so all 182 model regions were "due" at once and the hourly
    models (HRRR, RAP) waited hours behind the ensembles. Without a store, the server decides as before."""
    import hf_store
    if hf_store.enabled():
        return hf_store.has(model, region, run)
    if STORE:
        return _has(STORE, model, region, run)
    return _has(base, model, region, run, gone_ok=True)


def _has(base, model, region, run, gone_ok=False):
    import requests
    if not base:
        return gone_ok
    try:
        r = requests.get(f"{base}/models/{model}/{region}/{run}/manifest.json", timeout=60,
                         headers={"User-Agent": "gwcfc-model-builder"})
        if r.status_code != 200:
            return False
        man = r.json()
        return bool(man.get("fields")) and not man.get("partial")
    except requests.ConnectionError:
        return gone_ok
    except Exception:                                   # noqa: BLE001
        return False


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True)
    ap.add_argument("--region", default="conus")
    ap.add_argument("--run", help="YYYYMMDD_HH; the newest complete run when left out")
    ap.add_argument("--hours", help="only these forecast hours (a test), e.g. 0,1,2")
    ap.add_argument("--dry", action="store_true", help="build, do not upload")
    ap.add_argument("--force", action="store_true", help="build even when the server has the run")
    a = ap.parse_args(argv)

    base = (os.environ.get("GWCFC_BASE") or "").strip().rstrip("/")
    key = os.environ.get("GWCFC_UPLOAD_KEY") or ""
    if not a.dry and (not base or "PRIVATE KEY" not in key):
        # Not a failure: a scheduled build before the secrets are added would otherwise fail (and send a failure
        # e-mail) twice an hour. The note shows on the run's page.
        print("::warning::Nothing built: add the repository secrets GWCFC_BASE and GWCFC_UPLOAD_KEY "
              "(Settings > Secrets and variables > Actions).", flush=True)
        return 0
    key_path = None
    if not a.dry:
        fd, key_path = tempfile.mkstemp(prefix="k-")
        with os.fdopen(fd, "w") as fh:
            fh.write(key.strip() + "\n")
        os.chmod(key_path, 0o600)

    if not unlock_pipeline():
        print("::warning::Nothing built: the drawing code could not be unlocked (is GWCFC_UPLOAD_KEY the right key?)",
              flush=True)
        return 0
    import gfs_pipeline as G
    if a.model not in G.MODELS:
        log(f"no such model: {a.model}")
        return 2
    m0 = G.MODELS[a.model]
    m = G.region_spec(m0, a.region)
    if a.run:
        date_str, cyc = a.run.split("_")
    else:
        date_str, cyc = G.cycle_for(m)
        date_str, cyc = G.freshest_cycle(m, date_str, cyc, key=f"{a.model}/{a.region}")
        # The newest run that is all there: a run still arriving is left for a later start, and the one before it
        # is built instead if the server does not have it.
        from datetime import datetime, timedelta
        for _ in range(4):
            if G.run_is_complete(m, date_str, cyc):
                break
            t = datetime.strptime(date_str + cyc, "%Y%m%d%H") - timedelta(hours=int(m.get("cycle_h") or 6))
            date_str, cyc = t.strftime("%Y%m%d"), t.strftime("%H")
        else:
            print(f"::notice::{a.model}/{a.region}: no complete run in the last few cycles; nothing to do", flush=True)
            return 0
    run = f"{date_str}_{cyc}"
    if not a.dry and not a.force and server_has(base, a.model, a.region, run):
        log(f"{a.model}/{a.region} {run}: the server already has it whole; nothing to do")
        return 0
    if a.hours:
        keep = {int(h) for h in a.hours.split(",")}
        real = G.fhours_for
        G.fhours_for = lambda mm, c=None: [h for h in real(mm, c) if h in keep]

    t0 = time.time()
    log(f"{a.model}/{a.region}: building {run}")
    man = G._build_model(a.model, m0, a.region, run=(date_str, cyc))
    run_dir = os.path.join(G.OUT_DIR, a.model, a.region, run)
    if not man or man.get("run") != run or not os.path.exists(os.path.join(run_dir, "manifest.json")):
        print(f"::notice::{a.model}/{a.region} {run}: nothing was built (not published yet?)", flush=True)
        return 0
    files = [n for n in os.listdir(run_dir) if not n.startswith(".") and os.path.isfile(os.path.join(run_dir, n))]
    size = sum(os.path.getsize(os.path.join(run_dir, n)) for n in files)
    log(f"built {len(man.get('hours') or [])} hours, {len(man.get('fields') or {})} products, {len(files)} files, "
        f"{size / 1e6:.0f} MB in {time.time() - t0:.0f}s")
    if a.dry:
        return 0
    tw = time.time()
    ok, n = to_webp(run_dir)
    if ok:
        files = [n2 for n2 in os.listdir(run_dir) if not n2.startswith(".") and os.path.isfile(os.path.join(run_dir, n2))]
        size2 = sum(os.path.getsize(os.path.join(run_dir, n2)) for n2 in files)
        log(f"as WebP: {ok} pictures, {size2 / 1e6:.0f} MB (was {size / 1e6:.0f} MB) in {time.time() - tw:.0f}s")
    elif n:
        log(f"WebP did not work for every picture ({n}); sending the PNGs")

    import hf_store
    if hf_store.enabled():
        # Hugging Face holds the runs (hf_store.py): the web host is full and drops big uploads.
        t1 = time.time()
        hf_store.send_run(run_dir, a.model, a.region, run, log=log)
        log(f"uploaded in {time.time() - t1:.0f}s")
        try:
            os.unlink(key_path)
        except OSError:
            pass
        return 0
    t1 = time.time()
    n_parts = 0
    for i, names in enumerate(parts(run_dir)):
        post(base, key_path, a.model, a.region, run, "part", i, tar_of(run_dir, names))
        n_parts += 1
        if i % 10 == 0:
            log(f"  part {i} sent")
    out = post(base, key_path, a.model, a.region, run, "commit", n_parts, json.dumps({"files": len(files)}).encode())
    log(f"uploaded {n_parts} parts in {time.time() - t1:.0f}s; the server has {out.get('files')} files, "
        f"{out.get('hours')} hours of {run}")
    try:
        os.unlink(key_path)
    except OSError:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
