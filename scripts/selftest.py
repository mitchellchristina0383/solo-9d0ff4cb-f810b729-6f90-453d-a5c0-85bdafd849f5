#!/usr/bin/env python3
"""End-to-end self-test for the classifier service.

Two ways to run:

1. Fully local (spawns uvicorn on a random port with a temp SQLite file,
   SIGKILLs it mid-job and starts it again)::

       python scripts/selftest.py all

2. Against an already running server (e.g. ``docker compose up``)::

       python scripts/selftest.py --base-url http://localhost:8000
       python scripts/selftest.py --base-url http://localhost:8000 \\
           --restart-cmd "docker compose restart classifier-api"

Only the Python standard library is required on the host.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent

PASS = "✅ PASS"
FAIL = "❌ FAIL"
INFO = "ℹ️ "


class AssertFailed(Exception):
    pass


def check(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertFailed(msg)
    print(f"{PASS} {msg}")


class Client:
    def __init__(self, base_url: str):
        self.base = base_url.rstrip("/")

    def _request(
        self, method: str, path: str, body: dict | None = None,
        timeout: float = 30.0,
    ) -> tuple[int, Any]:
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            self.base + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8")
                return resp.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8")
            try:
                return e.code, json.loads(raw)
            except json.JSONDecodeError:
                return e.code, raw

    def get(self, path: str) -> Any:
        code, body = self._request("GET", path)
        if code != 200:
            raise AssertFailed(f"GET {path} -> {code}: {body}")
        return body

    def post(self, path: str, body: dict | None = None) -> tuple[int, Any]:
        return self._request("POST", path, body or {})

    def put(self, path: str, body: dict) -> Any:
        code, resp = self._request("PUT", path, body)
        if code != 200:
            raise AssertFailed(f"PUT {path} -> {code}: {resp}")
        return resp

    def wait_health(self, timeout: float = 30.0) -> dict:
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                return self.get("/health")
            except Exception as exc:  # noqa: BLE001
                last = exc
                time.sleep(0.25)
        raise AssertFailed(f"server never became healthy: {last}")

    def wait_terminal(
        self, job_id: str, timeout: float = 60.0
    ) -> dict[str, Any]:
        deadline = time.time() + timeout
        last_status = None
        while time.time() < deadline:
            job = self.get(f"/jobs/{job_id}")
            last_status = job["status"]
            if last_status in (
                "completed",
                "completed_with_failures",
                "error",
            ):
                return job
            time.sleep(0.2)
        raise AssertFailed(
            f"job {job_id} did not finish within {timeout}s "
            f"(last status {last_status})"
        )


def short_doc(i: int) -> dict:
    topics = [
        "央行银行金融财报投资股票",
        "球队比赛进球体育联赛",
        "芯片算法人工智能软件数据",
        "医院药物疫苗健康医学",
    ]
    return {"id": f"D-{i:03d}", "text": f"文档{i:03d}：{topics[i % 4]}。" * 3}


# ---------------------------------------------------------------- scenarios


def scenario_shrink(cli: Client) -> None:
    print("\n=== 场景 1：部分成功 → OOM 缩批 → 单篇失败 → 其余完成 ===")
    cli.put("/admin/node", {"vram_limit_mb": 512, "per_doc_delay_ms": 0})

    code, demo = cli.post("/demo/run")
    check(code in (200, 201), f"POST /demo/run 返回 {code}")
    job_id = demo["job_id"]
    check(job_id == "JOB-DEMO-SHRINK", "演示作业号为 JOB-DEMO-SHRINK")

    job = cli.wait_terminal(job_id)
    check(job["status"] == "completed_with_failures",
          f"作业终态为 completed_with_failures（实际 {job['status']}）")
    c = job["counts"]
    check(c["succeeded"] == 7 and c["failed"] == 1,
          f"7 成功 / 1 失败（实际 成功{c['succeeded']} 失败{c['failed']}）")
    check(c["pending"] == 0 and c["processing"] == 0,
          "没有遗留 pending/processing 文档")
    check(job["current_batch_size"] == 1,
          f"批大小已从 4 缩到 1（实际 {job['current_batch_size']}）")

    docs = job["documents"]
    check([d["seq"] for d in docs] == list(range(8)),
          "查询结果严格按提交顺序(seq)排列")
    failed = [d for d in docs if d["status"] == "failed"]
    check(len(failed) == 1 and failed[0]["doc_id"] == "DEMO-05",
          "唯一失败是超长单篇 DEMO-05")
    check(failed[0]["error_code"] == "document_exceeds_vram_limit",
          "失败码 document_exceeds_vram_limit")
    check("512MB" in (failed[0]["error_message"] or ""),
          "失败原因包含显存需求与上限说明")
    check(all(d["category"] for d in docs if d["status"] == "succeeded"),
          "成功文档都带分类结果")

    dispatches = cli.get(f"/jobs/{job_id}/dispatches")["dispatches"]
    first_batch_id = dispatches[0]["batch_id"]
    first_batch_seqs = sorted(
        d["seq"] for d in dispatches if d["batch_id"] == first_batch_id
    )
    check(first_batch_seqs == [0, 1, 2, 3],
          f"首次派发为整批 4 篇（实际 seq={first_batch_seqs}）")
    # DEMO-03 (seq2) was in the OOM batch and then re-dispatched smaller.
    seq2 = [d for d in dispatches if d["seq"] == 2]
    check(len(seq2) == 2 and seq2[0]["batch_id"] != seq2[1]["batch_id"],
          "触发 OOM 的未完成文档在更小批次中被重试")


def scenario_concurrent_resume(cli: Client, tag: str = "CONC") -> None:
    print("\n=== 场景 2：并发续跑去重（无重启部分） ===")
    cli.put("/admin/node", {"vram_limit_mb": 4096, "per_doc_delay_ms": 120})
    job_id = f"JOB-{tag}-{int(time.time())}"
    code, _body = cli.post("/jobs", {
        "job_id": job_id,
        "batch_size": 4,
        "vram_limit_mb": 4096,
        "documents": [short_doc(i) for i in range(12)],
    })
    check(code == 201, f"创建并发测试作业 {job_id}")
    # It auto-starts; hammer resume concurrently.
    with ThreadPoolExecutor(max_workers=10) as pool:
        responses = list(pool.map(
            lambda _i: cli.post(f"/jobs/{job_id}/resume"), range(10)
        ))
    codes = [c for c, _ in responses]
    check(all(c == 200 for c in codes), "10 个并发 resume 请求全部 200")
    started_true = sum(1 for _, b in responses if b.get("started") is True)
    check(started_true == 0,
          f"作业已自动运行，没有任何 resume 重复启动（started=true 数={started_true}）")

    job = cli.wait_terminal(job_id)
    check(job["counts"]["succeeded"] == 12, "12 篇全部成功")
    rows = cli.get(f"/jobs/{job_id}/dispatches")["dispatches"]
    seqs = [r["seq"] for r in rows]
    check(sorted(seqs) == list(range(12)) and len(set(seqs)) == 12,
          "每篇文档恰好派发一次，无重复派发")
    cli.put("/admin/node", {"vram_limit_mb": 512, "per_doc_delay_ms": 0})


def scenario_restart_resume(
    cli: Client,
    restart_fn: Callable[[], None] | None,
) -> None:
    print("\n=== 场景 3：kill -9 / 重启后按原作业号续跑 + 8 并发 resume ===")
    if restart_fn is None:
        print(f"{INFO}未提供重启手段，跳过本场景（本地 all 模式会完整执行）")
        return

    cli.put("/admin/node", {"vram_limit_mb": 4096, "per_doc_delay_ms": 150})
    job_id = f"JOB-RESTART-{int(time.time())}"
    code, _ = cli.post("/jobs", {
        "job_id": job_id,
        "batch_size": 4,
        "vram_limit_mb": 4096,
        "documents": [short_doc(i) for i in range(16)],
    })
    check(code == 201, f"创建重启测试作业 {job_id}")

    # Let some documents become terminal and at least one batch in flight.
    terminal_before: set[int] = set()
    max_dispatch_id = 0
    deadline = time.time() + 10
    while time.time() < deadline:
        job = cli.get(f"/jobs/{job_id}")
        terminal_before = {
            d["seq"] for d in job["documents"]
            if d["status"] in ("succeeded", "failed")
        }
        rows = cli.get(f"/jobs/{job_id}/dispatches")["dispatches"]
        max_dispatch_id = max((r["id"] for r in rows), default=0)
        if terminal_before and job["counts"]["processing"] > 0:
            break
        time.sleep(0.1)
    check(len(terminal_before) >= 1,
          f"kill 前已有终态文档 {sorted(terminal_before)}")
    print(f"{INFO}kill 前终态 seq={sorted(terminal_before)}，"
          f"dispatch id 上界={max_dispatch_id}")

    restart_fn()
    health = cli.wait_health(timeout=40)
    reset_info = health.get("stale_reset_on_startup") or {}
    check(reset_info.get("reset_documents", 0) >= 1,
          f"启动时把孤儿 processing 重置为 pending：{reset_info}")

    job_after_boot = cli.get(f"/jobs/{job_id}")
    check(job_after_boot["status"] == "interrupted",
          f"崩溃作业被标记 interrupted（实际 {job_after_boot['status']}）")

    # Concurrent resumes against the SAME original job id.
    barrier = threading.Barrier(8)

    def do_resume(_i):
        barrier.wait()
        return cli.post(f"/jobs/{job_id}/resume")

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(do_resume, range(8)))
    check(all(c == 200 for c, _ in responses), "8 个并发续跑请求全部 200")
    started = [b.get("started") for _, b in responses]
    check(sum(1 for s in started if s is True) == 1,
          f"恰好 1 个请求真正启动续跑（started 分布={started}）")

    job = cli.wait_terminal(job_id, timeout=60)
    check(job["status"] == "completed",
          f"续跑后作业 completed（实际 {job['status']}）")
    check(job["counts"]["succeeded"] == 16, "16 篇全部成功")

    rows = cli.get(f"/jobs/{job_id}/dispatches")["dispatches"]
    new_rows = [r for r in rows if r["id"] > max_dispatch_id]
    leaked = sorted({r["seq"] for r in new_rows} & terminal_before)
    check(not leaked,
          f"重启前已终态的文档在重启后零派发（泄漏 seq={leaked}）")
    new_seqs = [r["seq"] for r in new_rows]
    check(len(new_seqs) == len(set(new_seqs)),
          f"并发续跑没有重复派发同一文档（重启后派发 seq={sorted(new_seqs)}）")
    print(f"{INFO}重启后新派发 {len(new_rows)} 篇：{sorted(set(new_seqs))}")
    cli.put("/admin/node", {"vram_limit_mb": 512, "per_doc_delay_ms": 0})


# ------------------------------------------------- local uvicorn orchestration


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def run_all_local() -> int:
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    tmpdir = tempfile.mkdtemp(prefix="cls-selftest-")
    db_path = str(Path(tmpdir) / "selftest.db")
    log_path = str(Path(tmpdir) / "uvicorn.log")

    env = dict(os.environ)
    env.update({
        "DB_PATH": db_path,
        "VRAM_LIMIT_MB": "512",
        "BATCH_SIZE": "4",
        "NODE_DELAY_MS": "0",
        "PYTHONPATH": str(REPO_ROOT),
    })
    python = sys.executable
    log_files: list = []

    def spawn() -> subprocess.Popen:
        # Intentionally long-lived for the whole server lifetime; the
        # handles are closed in the final cleanup below.
        log = open(log_path, "ab")  # noqa: SIM115
        log_files.append(log)
        return subprocess.Popen(
            [python, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(port),
             "--log-level", "warning"],
            cwd=str(REPO_ROOT), env=env,
            stdout=log, stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    proc = spawn()
    cli = Client(base_url)
    holder = [proc]

    def hard_restart() -> None:
        current = holder[0]
        print(f"{INFO}SIGKILL uvicorn (pid={current.pid}) 模拟进程崩溃 ...")
        try:
            os.killpg(os.getpgid(current.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            current.kill()
        current.wait(timeout=10)
        time.sleep(0.5)
        holder[0] = spawn()

    failures: list[str] = []
    try:
        health = cli.wait_health()
        print(f"{PASS}本地 uvicorn 已启动：{base_url} db={db_path}")
        print(f"{INFO}/health -> {health}")

        for fn in (
            lambda: scenario_shrink(cli),
            lambda: scenario_concurrent_resume(cli),
            lambda: scenario_restart_resume(cli, hard_restart),
        ):
            try:
                fn()
            except Exception as exc:  # noqa: BLE001
                failures.append(str(exc))
                print(f"{FAIL} {exc}")
    finally:
        current = holder[0]
        try:
            os.killpg(os.getpgid(current.pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            current.kill()
        try:
            current.wait(timeout=10)
        except Exception:  # noqa: BLE001
            current.kill()
        for fh in log_files:
            fh.close()

    print("\n================ 自测总结 ================")
    if failures:
        print(f"{FAIL} {len(failures)} 个场景失败：")
        for f in failures:
            print(f" - {f}")
        print(f"{INFO}uvicorn 日志：{log_path}")
        return 1
    print("全部场景通过：缩批重试、单篇失败隔离、并发去重、kill -9 重启续跑。")
    print(f"{INFO}临时数据库保留在 {db_path}（可手动删除 {tmpdir}）")
    return 0


def run_against(base_url: str, restart_cmd: str | None) -> int:
    cli = Client(base_url)
    health = cli.wait_health()
    print(f"{PASS}目标服务健康：{base_url} -> {health}")

    restart_fn: Callable[[], None] | None = None
    if restart_cmd:
        def restart_fn() -> None:  # type: ignore[no-redef]
            print(f"{INFO}执行重启命令：{restart_cmd}")
            rc = subprocess.run(
                restart_cmd, check=False, shell=True, cwd=str(REPO_ROOT)
            ).returncode
            if rc != 0:
                raise AssertFailed(f"重启命令退出码 {rc}")
            time.sleep(1.0)

    failures: list[str] = []
    for fn in (
        lambda: scenario_shrink(cli),
        lambda: scenario_concurrent_resume(cli),
        lambda: scenario_restart_resume(cli, restart_fn),
    ):
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            failures.append(str(exc))
            print(f"{FAIL} {exc}")

    print("\n================ 自测总结 ================")
    if failures:
        print(f"{FAIL} {len(failures)} 个场景失败")
        return 1
    print("针对运行中服务的全部可用场景通过。")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mode", nargs="?", default="all",
        choices=["all", "remote"],
        help="all=自起 uvicorn 完整自测；remote=配合 --base-url",
    )
    parser.add_argument("--base-url", default=os.environ.get(
        "BASE_URL", "http://localhost:8000"))
    parser.add_argument("--restart-cmd", default=None,
                        help="remote 模式下用于重启服务的 shell 命令")
    args = parser.parse_args()

    if args.mode == "all":
        return run_all_local()
    return run_against(args.base_url, args.restart_cmd)


if __name__ == "__main__":
    sys.exit(main())
