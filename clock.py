#!/usr/bin/env python3
"""The clock: starts each model's build when its run is out, from a timetable of run times (schedule.json).

    python3 clock.py --once          # print what is due now, start nothing
    python3 clock.py --loop 320      # every 5 minutes for 320 minutes, starting what is due

For each model the timetable says when its runs start (UTC) and how long after that the whole run is usually out.
From 20 minutes before that time the clock asks, every 5 minutes, whether the run's last forecast hour is published.
The moment it is, and the server does not have the run, the model's build is started for exactly that run. If the
newest run is not out yet and the server lacks the one before it, that one is built instead, so nothing is left
behind after a gap.

GitHub's own timer (schedule:) is not relied on: it starts late or not at all on a quiet repository. The clock is
one small job that runs for a few hours and starts its successor before it ends.

It needs: GWCFC_BASE and GWCFC_UPLOAD_KEY (to ask the server and to unlock the code that knows each model's file
names), and GITHUB_TOKEN with "actions: write" to start workflows in this repository.
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import build as B

HERE = os.path.dirname(os.path.abspath(__file__))
EARLY_MIN = 20          # start asking this long before the timetable's time
TICK_S = 300
REDO_MIN = 25           # the same model is not started again within this long (a build that failed is retried then)
API = "https://api.github.com"


def log(msg):
    print(time.strftime("%H:%M:%S", time.gmtime()), msg, flush=True)


def load(repo=""):
    """The timetable: the copy on the main branch when it can be read (so an edit to schedule.json is picked up by a
    clock that is already running), else the one this job checked out."""
    if repo:
        try:
            import requests
            r = requests.get(f"https://raw.githubusercontent.com/{repo}/main/schedule.json", timeout=30)
            if r.status_code == 200 and isinstance(r.json().get("models"), list):
                return r.json()
        except Exception:                               # noqa: BLE001
            pass
    with open(os.path.join(HERE, "schedule.json")) as fh:
        return json.load(fh)


def runs_due(entry, now):
    """The two newest runs of a model whose time to look has come, newest first, as (date, cyc) pairs."""
    cycles = list(range(24)) if entry["cycles"] == "hourly" else sorted(entry["cycles"])
    t = (now - timedelta(minutes=max(0, entry["ready_min"] - EARLY_MIN))).replace(minute=0, second=0, microsecond=0)
    out = []
    for _ in range(72):
        if t.hour in cycles:
            out.append((t.strftime("%Y%m%d"), f"{t.hour:02d}"))
            if len(out) == 2:
                break
        t -= timedelta(hours=1)
    return out


def due(sched, base, G, now, complete_cache):
    """[{t, run}] to build now: for each model, the newest published run the server does not have."""
    out = []
    for e in sched["models"]:
        model, region = e["t"].split("/")
        if model not in G.MODELS:
            continue
        m = G.region_spec(G.MODELS[model], region)
        for date_str, cyc in runs_due(e, now):
            run = f"{date_str}_{cyc}"
            if B.server_has(base, model, region, run):
                break                                   # the server has this one: nothing older is wanted
            key = (e["t"], run)
            if key not in complete_cache:
                try:
                    ok = bool(G.run_is_complete(m, date_str, cyc))
                except Exception:                       # noqa: BLE001
                    ok = False
                if ok:
                    complete_cache[key] = True
            if complete_cache.get(key):
                out.append({"t": e["t"], "run": run})
                break
    return out


def gh(method, path, token, **kw):
    import requests
    return requests.request(method, API + path, timeout=60, headers={
        "Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "gwcfc-model-builder"}, **kw)


def building(repo, token):
    """The model regions being built or waiting to be, read from the build workflow's unfinished jobs."""
    out = set()
    for status in ("in_progress", "queued"):
        try:
            r = gh("GET", f"/repos/{repo}/actions/workflows/build.yml/runs?status={status}&per_page=50", token)
            for run in (r.json().get("workflow_runs") or []):
                j = gh("GET", f"/repos/{repo}/actions/runs/{run['id']}/jobs?per_page=100", token)
                for job in (j.json().get("jobs") or []):
                    name = job.get("name") or ""
                    if job.get("status") != "completed" and name.startswith("build "):
                        out.add(name.split()[1].strip("(),"))
        except Exception:                               # noqa: BLE001
            pass
    return out


def start(repo, token, workflow, inputs=None):
    r = gh("POST", f"/repos/{repo}/actions/workflows/{workflow}/dispatches", token,
           json={"ref": "main", "inputs": inputs or {}})
    return r.status_code in (200, 201, 204)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--loop", type=int, default=0, help="minutes to keep going")
    a = ap.parse_args(argv)
    base = (os.environ.get("GWCFC_BASE") or "").strip().rstrip("/")
    token = os.environ.get("GITHUB_TOKEN") or ""
    repo = os.environ.get("GITHUB_REPOSITORY") or ""
    if not base or not B.unlock_pipeline():
        print("::warning::The clock did nothing: the repository secrets GWCFC_BASE and GWCFC_UPLOAD_KEY are needed.",
              flush=True)
        return 0 if a.once else 3            # not 0: a clock that cannot run must not start its successor
    import gfs_pipeline as G
    sched = load()
    cache = {}
    if a.once:
        print(json.dumps(due(sched, base, G, datetime.now(timezone.utc), cache)))
        return 0
    cap = int(sched.get("max_at_once") or 5)
    last = {}                                           # model region or workflow -> when it was last started
    end = time.time() + a.loop * 60
    while True:
        t0 = time.time()
        now = datetime.now(timezone.utc)
        try:
            sched = load(repo)
            cap = int(sched.get("max_at_once") or 5)
            for job in sched.get("jobs") or []:
                if t0 - last.get(job["workflow"], 0) >= job["every_min"] * 60 - 30:
                    if start(repo, token, job["workflow"]):
                        last[job["workflow"]] = t0
                        log(f"started {job['workflow']}")
            want = due(sched, base, G, now, cache)
            busy = building(repo, token)
            room = cap - len(busy)
            go = []
            for d in want:
                if d["t"] in busy or t0 - last.get(d["t"], 0) < REDO_MIN * 60:
                    continue
                if len(go) >= room:
                    break
                go.append(d)
            if go and start(repo, token, "build.yml", {"targets": json.dumps(go)}):
                for d in go:
                    last[d["t"]] = t0
            log(f"due {len(want)}, building {len(busy)}, started {len(go)}: "
                + (", ".join(f"{d['t']} {d['run']}" for d in go) or "-"))
        except Exception as e:                          # noqa: BLE001
            log(f"tick failed: {e.__class__.__name__}")
        if time.time() + TICK_S > end:
            break
        time.sleep(max(5, TICK_S - (time.time() - t0)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
