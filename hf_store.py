"""Model runs kept on Hugging Face (the dataset gwcfc/gwcfc-models) instead of the web host.

The web host's plan is about 10 GB and was at twice that (Nov 2026), and it drops any request body over about 2 MB,
so runs could no longer be sent there at all. A public Hugging Face dataset holds files of any size, and anyone can
read them straight from the page (it allows other sites to read its files), so the page reads the models from it.

Layout, the same as the web host's store so the page reads both the same way:
  models/list.json                              every model and region, its newest run (the store's list.json shape)
  models/<model>/<region>/<run>/manifest.json   and the run's pictures beside it

One commit holds a run's files, the updated list and the removal of the older run, so the list never names a run
that is not all there. Builders run side by side, so the commit says which version of the list it started from
(parent_commit): when another builder committed first, the list is read again and the commit tried again.

The token is the repository secret HF_TOKEN (a Hugging Face write token). It is never printed.
"""
import json
import os
import time

REPO = os.environ.get("GWCFC_HF_REPO") or "gwcfc/gwcfc-models"
KEEP_RUNS = int(os.environ.get("GWCFC_HF_KEEP") or "1")
READ = f"https://huggingface.co/datasets/{REPO}/resolve/main"


SHARDS = 64   # Hugging Face takes at most 10,000 files in one folder; a GEFS member run has more (Nov 26, 2026)


def shard(name):
    """Which of the run's SHARDS subfolders a file goes in. The page works out the same number (_hfShard)."""
    h = 0
    for c in name:
        h = (h * 31 + ord(c)) & 0xFFFFFFFF
    return "s%02d" % (h % SHARDS)


def enabled():
    return bool((os.environ.get("HF_TOKEN") or "").strip())


def _api():
    from huggingface_hub import HfApi
    api = HfApi(token=os.environ["HF_TOKEN"].strip())
    api.create_repo(REPO, repo_type="dataset", exist_ok=True, private=False)
    return api


def read_list():
    """The list as it is now, or an empty one."""
    import requests
    try:
        r = requests.get(f"{READ}/models/list.json", params={"t": int(time.time())}, timeout=60,
                         headers={"User-Agent": "gwcfc-model-builder"})
        if r.status_code == 200:
            return r.json()
    except Exception:                                   # noqa: BLE001
        pass
    return {"models": {}}


def has(model, region, run):
    e = ((read_list().get("models") or {}).get(model) or {}).get("regions", {}).get(region) or {}
    return e.get("run") == run or run in (e.get("runs") or [])


def _entry(idx, model, region, run, man):
    models = idx.setdefault("models", {})
    head = models.get(model)
    if not isinstance(head, dict):
        head = {"label": man.get("label") or model, "res": man.get("res") or "", "regions": {}}
    regions = head.setdefault("regions", {})
    old = regions.get(region) if isinstance(regions.get(region), dict) else {}
    runs = sorted(set([run] + list(old.get("runs") or [])), reverse=True)
    keep, gone = runs[:KEEP_RUNS], runs[KEEP_RUNS:]
    newest = keep[0]
    ent = dict(old)
    ent.update({"label": man.get("label") or old.get("label") or head.get("label") or model,
                "res": man.get("res") or old.get("res") or head.get("res") or ""})
    if newest == run:
        ent["run"] = run
        ent["shard"] = SHARDS
        ent["cycle"] = man.get("cycle", ent.get("cycle", ""))
        ent["path"] = f"{model}/{region}/{run}/manifest.json"
        ent["fields"] = sorted((man.get("fields") or {}).keys())
        if man.get("bounds"):
            ent["bounds"] = man["bounds"]
        if man.get("ext") == "webp":
            ent["ext"] = "webp"
        else:
            ent.pop("ext", None)
    ent["runs"] = keep
    for k in ("partial", "hours_done", "hours_planned", "slim"):
        ent.pop(k, None)
    regions[region] = ent
    models[model] = head
    idx["updated"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return gone


def send_run(run_dir, model, region, run, log=print):
    """Uploads one run and lists it. Returns the number of files sent."""
    from huggingface_hub import CommitOperationAdd, CommitOperationDelete
    from huggingface_hub.utils import HfHubHTTPError
    api = _api()
    with open(os.path.join(run_dir, "manifest.json")) as fh:
        man = json.load(fh)
    names = sorted(n for n in os.listdir(run_dir)
                   if not n.startswith(".") and os.path.isfile(os.path.join(run_dir, n)))
    prefix = f"models/{model}/{region}/{run}"
    adds = [CommitOperationAdd(path_in_repo=f"{prefix}/{n}" if n == "manifest.json" else f"{prefix}/{shard(n)}/{n}",
                               path_or_fileobj=os.path.join(run_dir, n)) for n in names]
    # The pictures are sent once, before the commit: a commit tried again after a clash only re-sends the list.
    api.preupload_lfs_files(REPO, additions=adds, repo_type="dataset")
    for attempt in range(12):
        head = api.repo_info(REPO, repo_type="dataset").sha
        idx = read_list()
        gone = _entry(idx, model, region, run, man)
        ops = list(adds)
        ops.append(CommitOperationAdd(path_in_repo="models/list.json",
                                      path_or_fileobj=json.dumps(idx, separators=(",", ":")).encode()))
        for g in gone:
            ops.append(CommitOperationDelete(path_in_repo=f"models/{model}/{region}/{g}/", is_folder=True))
        try:
            api.create_commit(REPO, operations=ops, repo_type="dataset", parent_commit=head,
                              commit_message=f"{model}/{region} {run}")
            log(f"on Hugging Face: {len(names)} files of {model}/{region} {run}"
                + (f", {len(gone)} older run(s) removed" if gone else ""))
            return len(names)
        except HfHubHTTPError as e:
            code = getattr(getattr(e, "response", None), "status_code", 0)
            if code in (409, 412, 429, 500, 502, 503):  # another builder committed first, or busy: read and try again
                time.sleep(min(60, 3 * 2 ** attempt))
                continue
            raise
        except ValueError:                              # an old run folder already gone: list read again
            time.sleep(3)
            continue
    raise RuntimeError(f"Hugging Face commit of {model}/{region} {run} did not go through")
