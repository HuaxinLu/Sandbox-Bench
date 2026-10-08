#!/usr/bin/env python3
"""agentenv_startup_bench.py -- AgentENV sandbox startup benchmark via the E2B SDK.

Same CLI and same output as agentenv_startup_bench.sh, but sandboxes are created
and killed through the in-process E2B Python SDK instead of the `aenv` CLI:

    shell:  aenv start <template> -d --timeout T   /   aenv delete <id>
    python: AsyncSandbox.create(template=..., timeout=T)  /  sb.kill()

Interface parity (same flags, same semantics, same printed lines):
    -t/--template  -n/--concurrency  --ttl|--timeout  --cli-timeout
    --preheat none|batch|full  --preheat-conc  --preheat-rounds  --preheat-only
    --page-cache  --page-cache-dir  --page-cache-conc
    --drop-caches  --cold-pool  --hold  --keep  -q/--quiet  --dry-run  -h/--help

Methodology (same as the shell version, handover doc section 8):
  * control plane judged ONLY by a direct HTTP GET /health (never a CLI);
  * concurrency is real: N in-flight creates, no artificial barrier;
  * a failed create can leak a sandbox -> scrape its UUID from the error text;
  * nothing is persisted: results go to stdout.

Connection: E2B_API_KEY / E2B_API_URL / E2B_SANDBOX_URL are taken from the
environment; the API key falls back to /var/lib/aenv/secrets/api-key and the
URLs to http://127.0.0.1:8000.
"""

from __future__ import annotations

import argparse
import asyncio
import glob
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

API = "http://127.0.0.1:8000"
API_KEY_FILE = "/var/lib/aenv/secrets/api-key"
CATALOG_DIR = "/vdb/aenv-snapshot-store/repository/catalog"
PAGE_CACHE_DIR = "/vdb/aenv-snapshot-store/repository/managed-layers"
UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

# ------------------------------------------------------------------ options
P = argparse.ArgumentParser(
    prog="agentenv_startup_bench.py",
    description="AgentENV sandbox startup benchmark via the E2B SDK (template only)",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog=(
        "examples:\n"
        "  agentenv_startup_bench.py -t demo-requests-1142 -n 100\n"
        "  agentenv_startup_bench.py -t ctf-p0-base -n 500 --preheat full --hold 30\n"
        "  agentenv_startup_bench.py -t ubuntu -n 100 -q\n"
    ),
)
P.add_argument("-t", "--template", required=True, help="template name (a snapshot alias works too)")
P.add_argument("-n", "--concurrency", type=int, default=100, help="concurrency (default 100)")
P.add_argument("--ttl", "--timeout", dest="ttl", type=int, default=3600, help="sandbox TTL seconds (default 3600)")
P.add_argument("--cli-timeout", type=int, default=1800, help="per-create client timeout (default 1800)")
P.add_argument("--preheat", choices=["none", "batch", "full"], default="none", help="preheat mode (default none)")
P.add_argument("--preheat-conc", type=int, default=32, help="batch preheat concurrency (default 32)")
P.add_argument("--preheat-rounds", type=int, default=1, help="batch preheat rounds (default 1)")
P.add_argument("--preheat-only", action="store_true", help="preheat only, no measurement round")
P.add_argument("--page-cache", action="store_true", help="read the template's layer files into the page cache")
P.add_argument("--page-cache-dir", default=PAGE_CACHE_DIR, help="layer directory")
P.add_argument("--page-cache-conc", type=int, default=16, help="page-cache read concurrency (default 16)")
P.add_argument("--drop-caches", action="store_true", help="drop page caches before the measurement round")
P.add_argument("--cold-pool", action="store_true", help="restart aenv first to cool the device pool")
P.add_argument("--hold", type=int, default=0, help="keep sandboxes alive N seconds after the run")
P.add_argument("--keep", action="store_true", help="keep measurement sandboxes")
P.add_argument("-q", "--quiet", action="store_true", help="print only the results")
P.add_argument("--dry-run", action="store_true", help="print the actions instead of running them")
P.add_argument("--api-url", default=API, help="control-plane URL for the /health probe")
ARGS = P.parse_args()
API = ARGS.api_url


# ------------------------------------------------------------------ output
def say(msg: str) -> None:
    if not ARGS.quiet:
        print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def die(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] error: {msg}", file=sys.stderr, flush=True)
    raise SystemExit(1)


def setup_sdk_env() -> None:
    """E2B SDK reads these at import/call time; set them before importing e2b."""
    if not os.environ.get("E2B_API_KEY"):
        try:
            key = Path(API_KEY_FILE).read_text().strip()
            if key:
                os.environ["E2B_API_KEY"] = key
        except OSError:
            pass
    if not os.environ.get("E2B_API_KEY"):
        die("E2B_API_KEY is not set and could not be read from " + API_KEY_FILE)
    os.environ.setdefault("E2B_API_URL", API)
    os.environ.setdefault("E2B_SANDBOX_URL", API)


# ------------------------------------------------------------------ metrics
def read_metric(name: str):
    if name == "pool":
        return len(glob.glob("/dev/ublkb*"))
    if name == "mem_avail":
        for line in open("/proc/meminfo"):
            if line.startswith("MemAvailable"):
                return int(line.split()[1]) // 1024          # MB
        return 0
    if name == "allocstall":
        total = 0
        for line in open("/proc/vmstat"):
            if line.startswith("allocstall"):
                total += int(line.split()[1])
        return total
    if name == "pgsteal":
        for line in open("/proc/vmstat"):
            if line.startswith("pgsteal_direct"):
                return int(line.split()[1])
        return 0
    if name == "vdb_sec_read":
        for line in open("/proc/diskstats"):
            parts = line.split()
            if len(parts) > 5 and parts[2] == "vdb":
                return int(parts[5])                          # sectors read
        return 0
    raise KeyError(name)


def page_cache_gb() -> float:
    for line in open("/proc/meminfo"):
        if line.startswith("Cached:"):
            return int(line.split()[1]) / 1048576
    return 0.0


# ------------------------------------------------------------------ helpers
def http_get(url: str, timeout: float = 10.0):
    """Returns (rc, code). rc=124 mimics the shell probe's client timeout."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return 0, str(resp.status)
    except urllib.error.HTTPError as exc:
        return 0, str(exc.code)
    except Exception:
        return 124, "000"


def drop_page_caches() -> None:
    if ARGS.dry_run:
        say("drop_caches: [dry-run]")
        return
    os.sync()
    try:
        with open("/proc/sys/vm/drop_caches", "w") as fh:
            fh.write("3\n")
    except OSError as exc:
        say(f"drop_caches: failed ({exc})")
        return
    say(f"drop_caches: cached={page_cache_gb():.1f}G")


def cold_pool_restart() -> None:
    if ARGS.dry_run:
        say("cold pool: [dry-run] restart aenv and wait for :8000")
        return
    say("cold pool: restarting aenv")
    for path in ("/vdb/aenv-persisted-tmpfs",):
        try:
            subprocess.run(["chown", "aenv:aenv", path], check=False)
            subprocess.run(["chmod", "750", path], check=False)
        except OSError:
            pass
    subprocess.run(["systemctl", "stop", "aenv"], check=False)
    for _ in range(20):
        if subprocess.run(["pgrep", "-x", "uvm-ublk-daemon"], capture_output=True).returncode != 0:
            break
        time.sleep(3)
    subprocess.run(["systemctl", "reset-failed", "aenv"], check=False)
    subprocess.run(["systemctl", "start", "aenv"], check=False)
    for _ in range(40):
        out = subprocess.run(["ss", "-ltn"], capture_output=True, text=True).stdout
        if ":8000 " in out:
            break
        time.sleep(3)


def wait_for_service() -> None:
    if ARGS.dry_run:
        return
    for _ in range(10):
        rc, _code = http_get(API + "/health", timeout=3)
        if rc == 0 and _code == "204":
            break
        time.sleep(1)
    else:
        die(f"control plane not reachable: {API}/health")
    _rc, code = http_get(API + "/health", timeout=5)
    active = subprocess.run(["systemctl", "is-active", "aenv"], capture_output=True, text=True).stdout.strip()
    say(f"service: {active or 'unknown'} /health={code}")


def resolve_record(name: str) -> Path | None:
    """Snapshot aliases live in catalog/aliases; templates only have a record."""
    alias = Path(CATALOG_DIR) / "aliases" / name
    if alias.exists():
        return Path(CATALOG_DIR) / "records" / (alias.read_text().strip().strip('"') + ".json")
    for record in Path(CATALOG_DIR, "records").glob("*.json"):
        try:
            if json.loads(record.read_text()).get("alias") == name:
                return record
        except Exception:
            continue
    return None


def page_cache_preheat() -> None:
    if ARGS.dry_run:
        say(f"page_cache: [dry-run] read layer files of {ARGS.template}")
        return
    record = resolve_record(ARGS.template)
    if record is None or not record.exists():
        say(f"page_cache: cannot resolve '{ARGS.template}' as a snapshot alias or template, skipped")
        return

    digests: set[str] = set()

    def walk(node) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "digest" and isinstance(value, str) and value.startswith("sha256:"):
                    digests.add(value.split(":", 1)[1])
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    try:
        walk(json.loads(record.read_text()))
    except Exception as exc:
        say(f"page_cache: cannot read record for '{ARGS.template}' ({type(exc).__name__}), skipped")
        return

    files = [Path(ARGS.page_cache_dir) / f"sha256_{d}.overlaybd.commit" for d in digests]
    files = [f for f in files if f.exists()]
    total = sum(f.stat().st_size for f in files)

    def rd(path: Path) -> None:
        try:
            with open(path, "rb") as fh:
                while fh.read(8 << 20):
                    pass
        except OSError:
            pass

    before, t0 = page_cache_gb(), time.perf_counter()
    if files:
        with ThreadPoolExecutor(max_workers=max(1, ARGS.page_cache_conc)) as pool:
            list(pool.map(rd, files))
    elapsed = max(time.perf_counter() - t0, 1e-6)
    say(f"page_cache: {len(files)}/{len(digests)} layer files, {total / 2**30:.2f} GB, "
        f"{elapsed:.1f}s, cached {before:.1f}G -> {page_cache_gb():.1f}G")


# ------------------------------------------------------------------ benchmark
async def create_one(index: int, out: list, live: list) -> None:
    """One sandbox create; records wall ms, ok, sandbox id, waitready flag."""
    from e2b import AsyncSandbox

    t0 = time.perf_counter()
    try:
        sandbox = await asyncio.wait_for(
            AsyncSandbox.create(template=ARGS.template, timeout=ARGS.ttl),
            timeout=ARGS.cli_timeout,
        )
        out[index] = {
            "ms": int((time.perf_counter() - t0) * 1000),
            "ok": True,
            "sid": sandbox.sandbox_id,
            "waitready": False,
        }
        live.append(sandbox)
    except Exception as exc:                     # noqa: BLE001 - report, never raise
        text = f"{type(exc).__name__}: {exc}"
        match = UUID_RE.search(text)
        out[index] = {
            "ms": int((time.perf_counter() - t0) * 1000),
            "ok": False,
            "sid": match.group(0) if match else "-",
            "waitready": "WaitReady" in text,
            "error": text.splitlines()[0][:160],
        }


async def kill_sandboxes(sandboxes: list) -> None:
    async def one(sandbox) -> None:
        try:
            await asyncio.wait_for(sandbox.kill(), timeout=60)
        except Exception:
            pass

    if sandboxes:
        await asyncio.gather(*[one(sb) for sb in sandboxes])


async def kill_ids(ids: list[str]) -> None:
    from e2b import AsyncSandbox

    async def one(sid: str) -> None:
        try:
            sandbox = await asyncio.wait_for(AsyncSandbox.connect(sid), timeout=30)
            await asyncio.wait_for(sandbox.kill(), timeout=60)
        except Exception:
            pass

    wanted = sorted({i for i in ids if i and i != "-"})
    if wanted:
        await asyncio.gather(*[one(sid) for sid in wanted])


async def heartbeat(pending: list, total: int) -> None:
    start = time.perf_counter()
    while True:
        await asyncio.sleep(10)
        done = sum(1 for task in pending if task.done())
        say(f"waiting: {done}/{total} done, {time.perf_counter() - start:.0f}s elapsed")


async def run_round(conc: int) -> tuple[list, list]:
    if ARGS.dry_run:
        say(f"[dry-run] {conc} x AsyncSandbox.create(template={ARGS.template}, timeout={ARGS.ttl})")
        return [], [], []

    out: list = [None] * conc
    live: list = []
    tasks = [asyncio.create_task(create_one(i, out, live)) for i in range(conc)]
    beat = asyncio.create_task(heartbeat(tasks, conc))
    try:
        await asyncio.gather(*tasks)
    finally:
        beat.cancel()
        try:
            await beat
        except asyncio.CancelledError:
            pass
    leaked = [r["sid"] for r in out if r and not r["ok"]]
    return out, live, leaked


async def preheat_round(conc: int, rnd: int) -> None:
    out, live, leaked = await run_round(conc)
    await kill_sandboxes(live)
    await kill_ids(leaked)
    if not ARGS.dry_run:
        say(f"preheat r{rnd}: {conc} starts, pool={read_metric('pool')}")


async def main() -> int:
    if ARGS.concurrency < 1:
        die("concurrency must be >= 1")
    if ARGS.hold < 0:
        die("--hold must be a non-negative integer")

    say(f"template={ARGS.template} conc={ARGS.concurrency}")
    say(f"preheat={ARGS.preheat} page_cache={int(ARGS.page_cache)} "
        f"drop_caches={int(ARGS.drop_caches)} hold={ARGS.hold}s dry_run={int(ARGS.dry_run)}")

    setup_sdk_env()
    import e2b  # noqa: F401  (import after env setup)

    if ARGS.cold_pool:
        cold_pool_restart()
    wait_for_service()
    say(f"start cmd: AsyncSandbox.create(template={ARGS.template}, timeout={ARGS.ttl})")

    measure_live: list = []
    measure_out: list = []
    measure_leaked: list = []
    failed = False
    try:
        if ARGS.preheat != "none" or ARGS.page_cache:
            say(f"preheat start: pool={read_metric('pool')} cached={page_cache_gb():.1f}G")
            if ARGS.preheat == "batch":
                for rnd in range(1, ARGS.preheat_rounds + 1):
                    await preheat_round(ARGS.preheat_conc, rnd)
            elif ARGS.preheat == "full":
                await preheat_round(ARGS.concurrency, 0)
            else:
                say("preheat: none")
            if ARGS.page_cache:
                page_cache_preheat()
            say(f"preheat done: pool={read_metric('pool')} cached={page_cache_gb():.1f}G")

        if ARGS.preheat_only:
            say("preheat only, no measurement round")
            return 0

        if ARGS.drop_caches:
            drop_page_caches()

        pool_before = read_metric("pool")
        mem_before = read_metric("mem_avail")
        alloc_before = read_metric("allocstall")
        pgsteal_before = read_metric("pgsteal")
        read_before = read_metric("vdb_sec_read")
        say(f"baseline: pool={pool_before} mem={mem_before}MB cached={page_cache_gb():.1f}G")

        samples: list = []
        stop = asyncio.Event()

        async def probe() -> None:
            while not stop.is_set():
                t0 = time.perf_counter()
                rc, code = await asyncio.to_thread(http_get, API + "/health", 10)
                samples.append((rc, code, int((time.perf_counter() - t0) * 1000)))
                try:
                    await asyncio.wait_for(stop.wait(), timeout=1)
                except asyncio.TimeoutError:
                    pass

        probe_task = None
        if not ARGS.dry_run:
            probe_task = asyncio.create_task(probe())
            await asyncio.sleep(2)
            say("control-plane probe running (1s interval)")

        say(f"starting {ARGS.concurrency} sandbox(es)")
        t0 = time.perf_counter()
        measure_out, measure_live, measure_leaked = await run_round(ARGS.concurrency)
        wall_ms = int((time.perf_counter() - t0) * 1000)
        say(f"all returned, wall {wall_ms}ms")

        if ARGS.hold > 0:
            if ARGS.dry_run:
                say(f"[dry-run] hold {ARGS.hold}s with sandboxes alive")
            else:
                say(f"hold {ARGS.hold}s with sandboxes alive")
                await asyncio.sleep(ARGS.hold)

        stop.set()
        if probe_task is not None:
            await probe_task

        pool_after = read_metric("pool")
        mem_after = read_metric("mem_avail")
        alloc_after = read_metric("allocstall")
        pgsteal_after = read_metric("pgsteal")
        read_after = read_metric("vdb_sec_read")
        vdb_gb = (read_after - read_before) * 512 / 2**30

        # ---------------------------------------------------------- results
        say("==================== results ====================")
        print(f"template    : {ARGS.template}   concurrency {ARGS.concurrency}")
        hold_note = f" (+{ARGS.hold}s hold)" if ARGS.hold > 0 else ""
        print(f"wall        : {wall_ms} ms{hold_note}")

        rows = [r for r in measure_out if r]
        ok = sorted(r["ms"] / 1000.0 for r in rows if r["ok"])
        waitready = sum(1 for r in rows if r["waitready"])
        print(f"ok/fail     : {len(ok)}/{len(rows) - len(ok)}   waitready_fail: {waitready}")
        if ok:
            def q(p: float) -> float:
                return round(ok[min(int(len(ok) * p), len(ok) - 1)], 2)

            buckets = {"<5s": 0, "5-10s": 0, "10-20s": 0, "20-60s": 0, ">=60s": 0}
            for secs in ok:
                key = ("<5s" if secs < 5 else "5-10s" if secs < 10 else
                       "10-20s" if secs < 20 else "20-60s" if secs < 60 else ">=60s")
                buckets[key] += 1
            print(f"latency     : p50 {q(.5)}s  p90 {q(.9)}s  p95 {q(.95)}s  p99 {q(.99)}s  "
                  f"max {round(ok[-1], 2)}s  >=10s: {sum(1 for s in ok if s >= 10)}")
            print("buckets     : " + "  ".join(f"{k} {v}" for k, v in buckets.items()))

        non204 = sum(1 for rc, code, _ms in samples if code != "204")
        timeouts = sum(1 for rc, _code, _ms in samples if rc == 124)
        lat = sorted(ms for _rc, _code, ms in samples)
        p50 = lat[len(lat) // 2] if lat else 0
        print(f"control     : {len(samples)} samples  non204={non204}  timeout={timeouts}  "
              f"p50={p50}ms  max={lat[-1] if lat else 0}ms")
        print(f"pool        : {pool_before} -> {pool_after}")
        print(f"vdb read    : {vdb_gb:.2f} GB")
        print(f"memory      : {mem_before}MB -> {mem_after}MB  "
              f"allocstall +{alloc_after - alloc_before}  pgsteal_direct +{pgsteal_after - pgsteal_before}")
        say("================================================")

    except (KeyboardInterrupt, asyncio.CancelledError):
        failed = True
        say("interrupted")
    finally:
        await kill_sandboxes(measure_live)
        await kill_ids(measure_leaked)
        if ARGS.keep and measure_live:
            say("kept measurement sandboxes (--keep); preheat ones deleted")
            for sandbox in measure_live:
                print(sandbox.sandbox_id)
        elif not ARGS.dry_run:
            say("reclaimed sandboxes created by this run")
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except KeyboardInterrupt:
        print("", file=sys.stderr)
        raise SystemExit(130)
