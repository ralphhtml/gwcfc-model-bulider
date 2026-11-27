#!/usr/bin/env python3
"""WeatherNext 2 on the builder: one member (`member`), or the members' mean drawn and sent (`draw`).

The work is pipeline/wn2_run.py (the server's own code, kept here only inside pipeline.bin); this file only
unlocks it, picks the run, and sends the drawn run to Hugging Face the same way build.py does.
"""
import argparse
import json
import os
import sys
import time

import build as B


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=["member", "draw"])
    ap.add_argument("--member", type=int, default=0)
    ap.add_argument("--out", default="members")
    ap.add_argument("--members", default="members")
    ap.add_argument("--steps", type=int, default=40)
    a = ap.parse_args()
    if not B.unlock_pipeline():
        print("::warning::the drawing code could not be unlocked", flush=True)
        return 0
    import wn2_run as W
    os.makedirs(a.out, exist_ok=True)
    if a.what == "member":
        run_t = W.newest_run()
        run = run_t.strftime("%Y%m%d_%H")
        import hf_store
        if hf_store.enabled() and hf_store.has("wn2", "world", run):
            print(f"wn2 {run}: already on Hugging Face", flush=True)
            return 0
        W.run_member(run_t, a.member, a.steps, a.out)
        with open(os.path.join(a.out, "run.txt"), "w") as fh:
            fh.write(run)
        return 0
    runf = os.path.join(a.members, "run.txt")
    if not os.path.exists(runf) or not any(f.endswith(".nc") for f in os.listdir(a.members)):
        print("::notice::no WeatherNext 2 members to draw", flush=True)
        return 0
    run = open(runf).read().strip()
    import gfs_pipeline as G
    run_dir = W.draw(a.members, run, G.OUT_DIR)
    B.to_webp(run_dir)
    import hf_store
    t = time.time()
    hf_store.send_run(run_dir, "wn2", "world", run, log=lambda m: print(m, flush=True))
    print(f"sent in {time.time() - t:.0f}s", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
