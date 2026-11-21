#!/usr/bin/env python3
"""只读查询 API 的端到端自测。

前置条件：
- API 已在 BASE_URL（默认 http://127.0.0.1:8000）运行，POOL_SIZE=4；
- 可选：环境变量 PSQL 给出 psql 连接串（如
  "postgresql://app:app@127.0.0.1:5432/demo"），用于服务端核对
  （连接数上限、无残留活跃查询、无失败事务）；未提供则跳过这些检查。

用法：python3 tests/selftest.py
"""

from __future__ import annotations

import asyncio
import os
import random
import subprocess
import sys
import time
from contextlib import suppress

import httpx

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
POOL_SIZE = int(os.environ.get("POOL_SIZE", "4"))
PSQL = os.environ.get("PSQL")  # 例如 postgresql://app:app@127.0.0.1:5432/demo
# “拒绝新建连接、已有连接不受影响”的故障注入命令，用于建连失败后恢复
# 的用例。例如（容器内可写 pg_hba.conf 并 reload）：
#   DB_REJECT_CMD="docker compose exec -T db bash -c '...'"
#   DB_RESTORE_CMD="docker compose exec -T db bash -c '...'"
# 两者同时提供才会执行该用例；不提供则跳过。
DB_REJECT_CMD = os.environ.get("DB_REJECT_CMD")
DB_RESTORE_CMD = os.environ.get("DB_RESTORE_CMD")

failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}"
          + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(f"{name}: {detail}")


async def stats(client: httpx.AsyncClient) -> dict:
    return (await client.get("/pool/stats")).json()


def server_count(where: str = "") -> int | None:
    """通过 pg_stat_activity 做服务端核对；无 psql 时返回 None。"""
    if not PSQL:
        return None
    sql = ("SELECT count(*) FROM pg_stat_activity "
           "WHERE application_name = 'ro-api' " + where)
    try:
        out = subprocess.run(
            ["psql", PSQL, "-tAc", sql],
            capture_output=True, text=True, timeout=10)
        if out.returncode != 0:
            return None
        return int(out.stdout.strip())
    except Exception:
        return None


def db_control(action: str) -> None:
    """执行“拒绝新建连接 / 恢复”的命令。"""
    cmd = DB_REJECT_CMD if action == "reject" else DB_RESTORE_CMD
    print(f"  [db] {action}: {cmd}")
    out = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                         timeout=120)
    if out.returncode != 0:
        raise RuntimeError(f"db {action} failed: {out.stderr[-500:]}")


class Sampler:
    """高频采样 /pool/stats，校验结构性不变量。

    缩容时 target 可以低于已借出的连接数（查询要照常跑完），因此：
    - idle + in_use + cleaning == opened_connections（槽位不泄漏）；
    - opened_connections 不超过本次测试见过的最大目标（从不超建）；
    - in_use + cleaning 也不超过该最大目标（借出数受当时目标约束）；
    - opened > target 时 idle 必须为 0（收缩先关空闲、归还即关）。
    """

    def __init__(self, client: httpx.AsyncClient, initial_target: int | None = None):
        self.client = client
        self.violations: list[str] = []
        self.max_in_use = 0
        self.max_waiting = 0
        self.peak_target = initial_target if initial_target is not None else POOL_SIZE
        self._stop = asyncio.Event()

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                s = (await self.client.get("/pool/stats")).json()
                self.peak_target = max(self.peak_target, s["target"])
                self.max_in_use = max(self.max_in_use, s["in_use"])
                self.max_waiting = max(self.max_waiting, s["waiting"])
                opened = s["opened_connections"]
                total = s["idle"] + s["in_use"] + s["cleaning"]
                if total != opened:
                    self.violations.append(f"accounting mismatch: {s}")
                if opened > self.peak_target:
                    self.violations.append(
                        f"opened > peak target {self.peak_target}: {s}")
                if s["in_use"] + s["cleaning"] > self.peak_target:
                    self.violations.append(
                        f"borrowed > peak target {self.peak_target}: {s}")
                if opened > s["target"] and s["idle"] > 0:
                    self.violations.append(
                        f"idle kept while above target: {s}")
            except Exception as exc:  # noqa: BLE001
                self.violations.append(f"sampler error: {exc}")
            await asyncio.sleep(0.05)

    def stop(self) -> None:
        self._stop.set()


async def wait_target(
    client: httpx.AsyncClient, target: int, timeout: float = 20
) -> dict:
    """等池达成目标：target_reached 为 true 且 opened == target。"""
    deadline = time.monotonic() + timeout
    s = await stats(client)
    while time.monotonic() < deadline:
        if (s["target"] == target and s["opened_connections"] == target
                and s["target_reached"] and s["connecting"] == 0):
            break
        await asyncio.sleep(0.1)
        s = await stats(client)
    return s


async def set_target(client: httpx.AsyncClient, target: int) -> dict:
    r = await client.post("/pool/target", json={"target": target})
    check(f"set-target {target}: 200", r.status_code == 200, r.text)
    return r.json() if r.status_code == 200 else {}


async def wait_pool_idle(client: httpx.AsyncClient, timeout: float = 15) -> dict:
    deadline = time.monotonic() + timeout
    s = await stats(client)
    while time.monotonic() < deadline:
        if (s["idle"] == s["target"] and s["cleaning"] == 0
                and s["waiting"] == 0 and s["target_reached"]):
            break
        await asyncio.sleep(0.1)
        s = await stats(client)
    return s


# ---------------------------------------------------------------- 用例

async def t_normal(client):
    r = await client.post("/query/normal", json={"limit": 3})
    check("normal: 200", r.status_code == 200, r.text)
    if r.status_code == 200:
        check("normal: 3 rows", len(r.json().get("rows", [])) == 3, r.text)


async def t_queue(client):
    """8 个慢查询、池大小为当前目标（默认 4）：部分执行、部分排队。"""
    size = (await stats(client))["target"]
    sampler = Sampler(client)
    stask = asyncio.create_task(sampler.run())
    reqs = [asyncio.create_task(client.post("/query/slow", json={
        "delay_seconds": 2, "acquire_timeout": 30, "query_timeout": 30}))
        for _ in range(8)]
    await asyncio.sleep(0.7)
    mid = await stats(client)
    check("queue: in_use == target", mid["in_use"] == size, str(mid))
    check("queue: waiting == 8 - target", mid["waiting"] == 8 - size, str(mid))
    results = await asyncio.gather(*reqs)
    sampler.stop()
    await stask
    codes = [r.status_code for r in results]
    check("queue: all 8 finished 200", all(c == 200 for c in codes), str(codes))
    check("queue: invariants held", not sampler.violations,
          "; ".join(sampler.violations[:3]))
    s = await wait_pool_idle(client)
    check("queue: pool idle again", s["idle"] == s["target"], str(s))


async def t_acquire_timeout(client):
    """池被占满时，acquire_timeout=1 的请求应在约 1 秒后拿到 503。"""
    size = (await stats(client))["target"]
    blockers = [asyncio.create_task(client.post("/query/slow", json={
        "delay_seconds": 3, "acquire_timeout": 30, "query_timeout": 30}))
        for _ in range(size)]
    await asyncio.sleep(0.7)
    t0 = time.monotonic()
    r = await client.post("/query/slow", json={
        "delay_seconds": 0.1, "acquire_timeout": 1, "query_timeout": 5})
    dt = time.monotonic() - t0
    check("acquire-timeout: 503", r.status_code == 503,
          f"{r.status_code} {r.text}")
    check("acquire-timeout: ~1s", 0.8 < dt < 2.5, f"{dt:.2f}s")
    results = await asyncio.gather(*blockers)
    check("acquire-timeout: blockers finished",
          all(x.status_code == 200 for x in results),
          str([x.status_code for x in results]))
    await wait_pool_idle(client)


async def t_queue_cancel(client):
    """排队中的请求被客户端取消后，队列名额应立即释放。"""
    size = (await stats(client))["target"]
    blockers = [asyncio.create_task(client.post("/query/slow", json={
        "delay_seconds": 2.5, "acquire_timeout": 30, "query_timeout": 30}))
        for _ in range(size)]
    await asyncio.sleep(0.7)
    before = await stats(client)
    queued = asyncio.create_task(client.post("/query/slow", json={
        "delay_seconds": 0.1, "acquire_timeout": 30, "query_timeout": 5}))
    await asyncio.sleep(0.5)
    mid = await stats(client)
    check("cancel: waiting grew by 1",
          mid["waiting"] == before["waiting"] + 1, f"{before} -> {mid}")
    queued.cancel()  # 客户端放弃 -> 服务端应观测到断开
    with suppress(asyncio.CancelledError):
        await queued
    deadline = time.monotonic() + 5
    s = await stats(client)
    while time.monotonic() < deadline and s["waiting"] != before["waiting"]:
        await asyncio.sleep(0.1)
        s = await stats(client)
    check("cancel: queue slot released", s["waiting"] == before["waiting"],
          str(s))
    check("cancel: counter bumped",
          s["counters"]["queue_cancelled"] > before["counters"]["queue_cancelled"],
          str(s["counters"]))
    results = await asyncio.gather(*blockers)
    check("cancel: blockers still finish",
          all(x.status_code == 200 for x in results),
          str([x.status_code for x in results]))
    await wait_pool_idle(client)


async def t_query_timeout(client):
    """query_timeout=1 的慢查询：504；连接不得带伤复用，池应恢复。"""
    before = await stats(client)
    t0 = time.monotonic()
    r = await client.post("/query/slow", json={
        "delay_seconds": 10, "acquire_timeout": 5, "query_timeout": 1})
    dt = time.monotonic() - t0
    check("query-timeout: 504", r.status_code == 504,
          f"{r.status_code} {r.text}")
    check("query-timeout: ~1s", 0.8 < dt < 3, f"{dt:.2f}s")
    s = await wait_pool_idle(client)
    check("query-timeout: pool recovered", s["idle"] == s["target"], str(s))
    c, b = s["counters"], before["counters"]
    check("query-timeout: recycled or replaced",
          c["recycled"] + c["replaced"] > b["recycled"] + b["replaced"], str(c))
    n = server_count()
    if n is not None:
        check("query-timeout: server conns <= target", n <= s["target"],
              f"n={n}")
    active = server_count("AND state = 'active'")
    if active is not None:
        check("query-timeout: nothing running server-side", active == 0,
              f"active={active}")
    r = await client.post("/query/normal", json={"limit": 1})
    check("query-timeout: normal query after", r.status_code == 200, r.text)


async def t_sql_error(client):
    """两种 SQL 报错：连接须清理/更换后才能复用，后续查询不受影响。"""
    before = await stats(client)
    r = await client.post("/query/error", json={"mode": "autocommit"})
    check("sql-error: autocommit -> 400", r.status_code == 400,
          f"{r.status_code} {r.text}")
    r = await client.post("/query/error", json={"mode": "transaction"})
    check("sql-error: transaction -> 400", r.status_code == 400,
          f"{r.status_code} {r.text}")
    s = await wait_pool_idle(client)
    c, b = s["counters"], before["counters"]
    check("sql-error: 2 connections sanitized",
          c["recycled"] + c["replaced"] >= b["recycled"] + b["replaced"] + 2,
          f"{b} -> {c}")
    aborted = server_count("AND state = 'idle in transaction (aborted)'")
    if aborted is not None:
        check("sql-error: no failed transaction left", aborted == 0,
              f"aborted={aborted}")
    # 池里每条连接都可能被轮到，多发几个普通查询验证没有残留事务
    results = await asyncio.gather(*[
        client.post("/query/normal", json={"limit": 2}) for _ in range(8)])
    check("sql-error: normal queries after",
          all(x.status_code == 200 for x in results),
          str([x.status_code for x in results]))


async def t_disconnect(client):
    """客户端在慢查询执行中断开：连接不得带伤复用，池应恢复。"""
    before = await stats(client)
    req = asyncio.create_task(client.post("/query/slow", json={
        "delay_seconds": 10, "acquire_timeout": 5, "query_timeout": 30}))
    await asyncio.sleep(0.7)
    req.cancel()
    with suppress(asyncio.CancelledError):
        await req
    s = await wait_pool_idle(client)
    check("disconnect: pool recovered", s["idle"] == s["target"], str(s))
    check("disconnect: counter bumped",
          s["counters"]["client_disconnects"]
          > before["counters"]["client_disconnects"], str(s["counters"]))
    active = server_count("AND state = 'active'")
    if active is not None:
        check("disconnect: nothing running server-side", active == 0,
              f"active={active}")
    r = await client.post("/query/normal", json={"limit": 1})
    check("disconnect: normal query after", r.status_code == 200, r.text)


async def t_stress(client):
    """取消与归还交错的压力测试：连接数不得越限，其他请求照常完成。"""
    sampler = Sampler(client)
    stask = asyncio.create_task(sampler.run())
    rng = random.Random(42)

    async def one(i: int):
        kind = i % 5
        try:
            if kind == 0:
                return await client.post("/query/error", json={
                    "mode": "transaction" if i % 10 == 0 else "autocommit"})
            if kind in (1, 2):
                return await client.post("/query/slow", json={
                    "delay_seconds": rng.uniform(0.3, 1.5),
                    "acquire_timeout": rng.choice([0.5, 2, 30]),
                    "query_timeout": rng.choice([0.5, 2, 30])})
            return await client.post("/query/normal", json={
                "limit": 3,
                "acquire_timeout": rng.choice([0.5, 30]),
                "query_timeout": 10})
        except Exception:  # noqa: BLE001 - 客户端侧异常不计失败
            return None

    async def aborted(_: int):
        t = asyncio.create_task(client.post("/query/slow", json={
            "delay_seconds": 3, "acquire_timeout": 30, "query_timeout": 30}))
        await asyncio.sleep(rng.uniform(0.1, 0.8))
        t.cancel()
        with suppress(asyncio.CancelledError):
            await t

    tasks = ([one(i) for i in range(30)]
             + [aborted(i) for i in range(6)])
    results = await asyncio.gather(*tasks)
    sampler.stop()
    await stask
    ok = sum(1 for r in results
             if r is not None and r.status_code == 200)
    check("stress: some requests completed", ok >= 10, f"ok={ok}")
    check("stress: invariants held", not sampler.violations,
          "; ".join(sampler.violations[:5]))
    s = await wait_pool_idle(client, timeout=20)
    check("stress: pool fully recovered",
          s["idle"] == s["target"] and s["cleaning"] == 0 and s["waiting"] == 0,
          str(s))
    check("stress: opened == target", s["opened_connections"] == s["target"],
          str(s))
    n = server_count()
    if n is not None:
        check("stress: server conns == target", n == s["target"], f"n={n}")
    r = await client.post("/query/normal", json={"limit": 1})
    check("stress: normal query after", r.status_code == 200, r.text)
    print("  stress counters:", s["counters"])


async def t_target_validation(client):
    """/pool/target 只接受正整数；非法值返回 422 且不改变目标。"""
    before = await stats(client)
    for body in ({"target": 0}, {"target": -2}, {"target": 2.5},
                 {"target": "3"}, {"target": True}, {"target": None}, {}):
        r = await client.post("/pool/target", json=body)
        check(f"target-validation: {body} -> 422", r.status_code == 422,
              f"{r.status_code} {r.text[:100]}")
    s = await stats(client)
    check("target-validation: target unchanged",
          s["target"] == before["target"], f"{before['target']} -> {s['target']}")


async def t_shrink_idle(client):
    """缩容：多出的空闲连接立即关闭，服务端会话数随之下降。"""
    base = (await stats(client))["target"]
    await set_target(client, base + 3)
    s = await wait_target(client, base + 3)
    check("shrink: grown to base+3", s["opened_connections"] == base + 3, str(s))
    before = await stats(client)
    t0 = time.monotonic()
    r = await client.post("/pool/target", json={"target": base})
    dt = time.monotonic() - t0
    check("shrink: 200", r.status_code == 200, r.text)
    check("shrink: respond quickly", dt < 3, f"{dt:.2f}s")
    body = r.json()
    check("shrink: reports target", body["target"] == base, str(body))
    check("shrink: opened == target right away",
          body["opened_connections"] == base, str(body))
    check("shrink: closed_for_resize +3",
          body["counters"]["closed_for_resize"]
          - before["counters"]["closed_for_resize"] == 3, str(body["counters"]))
    n = server_count()
    if n is not None:
        check("shrink: server conns == target", n == base, f"n={n}")
    r = await client.post("/query/normal", json={"limit": 1})
    check("shrink: normal query still works", r.status_code == 200, r.text)


async def t_shrink_in_use(client):
    """缩容不打断已借出的查询：跑完的查询照常成功，归还的连接被关闭。"""
    base = (await stats(client))["target"]
    await set_target(client, base)
    sampler = Sampler(client)
    stask = asyncio.create_task(sampler.run())
    blockers = [asyncio.create_task(client.post("/query/slow", json={
        "delay_seconds": 2.5, "acquire_timeout": 30, "query_timeout": 30}))
        for _ in range(base)]
    await asyncio.sleep(0.6)
    mid = await stats(client)
    check("shrink-in-use: all borrowed", mid["in_use"] == base, str(mid))
    # 在查询进行中缩到 1：空闲为 0，只能等归还时关闭
    r = await client.post("/pool/target", json={"target": 1})
    check("shrink-in-use: 200", r.status_code == 200, r.text)
    body = r.json()
    check("shrink-in-use: in_use preserved", body["in_use"] == base, str(body))
    check("shrink-in-use: opened still high",
          body["opened_connections"] == base, str(body))
    # 打开数高于目标期间暂停新借出：新请求应排队（而非借到连接）
    extra = asyncio.create_task(client.post("/query/normal", json={
        "limit": 1, "acquire_timeout": 1.5, "query_timeout": 5}))
    await asyncio.sleep(0.5)
    during = await stats(client)
    check("shrink-in-use: new borrow paused",
          during["waiting"] >= 1 and during["in_use"] == base, str(during))
    results = await asyncio.gather(*blockers)
    check("shrink-in-use: in-flight queries all 200",
          all(x.status_code == 200 for x in results),
          str([x.status_code for x in results]))
    extra_res = await extra
    # 排队请求在目标达成后可被服务；也可能因暂停期超过 acquire_timeout 而 503
    check("shrink-in-use: queued request resolved",
          extra_res.status_code in (200, 503), extra_res.text)
    s = await wait_target(client, 1)
    sampler.stop()
    await stask
    check("shrink-in-use: reached target 1",
          s["opened_connections"] == 1 and s["idle"] == 1, str(s))
    check("shrink-in-use: invariants held", not sampler.violations,
          "; ".join(sampler.violations[:3]))
    check("shrink-in-use: 3 closed on release",
          s["counters"]["closed_for_resize"] >= 3, str(s["counters"]))
    n = server_count()
    if n is not None:
        check("shrink-in-use: server conns == 1", n == 1, f"n={n}")
    await set_target(client, base)
    await wait_target(client, base)


async def t_shrink_grow_interleaved(client):
    """缩容与扩容交错（含查询负载）：不变量始终成立，最终回到目标。"""
    base = (await stats(client))["target"]
    sampler = Sampler(client)
    stask = asyncio.create_task(sampler.run())

    async def workload(i: int):
        try:
            if i % 3 == 0:
                return await client.post("/query/slow", json={
                    "delay_seconds": 0.8, "acquire_timeout": 10,
                    "query_timeout": 20})
            return await client.post("/query/normal", json={
                "limit": 2, "acquire_timeout": 10, "query_timeout": 10})
        except Exception:  # noqa: BLE001
            return None

    reqs = [asyncio.create_task(workload(i)) for i in range(24)]
    targets = [1, 5, 2, 6, 3, base, 1, 4]
    applied = []
    for tgt in targets:
        r = await client.post("/pool/target", json={"target": tgt})
        applied.append(r.status_code)
        await asyncio.sleep(0.35)
    results = await asyncio.gather(*reqs)
    check("interleave: all set-target accepted",
          all(c == 200 for c in applied), str(applied))
    check("interleave: requests resolved",
          all(r is not None for r in results), "some clients errored")
    codes = [r.status_code for r in results if r is not None]
    check("interleave: no server errors",
          all(c in (200, 503) for c in codes), str(sorted(codes)))
    s = await wait_target(client, base)
    sampler.stop()
    await stask
    check("interleave: back to base target",
          s["opened_connections"] == base and s["idle"] == base, str(s))
    check("interleave: invariants held", not sampler.violations,
          "; ".join(sampler.violations[:3]))
    r = await client.post("/query/normal", json={"limit": 1})
    check("interleave: normal query after", r.status_code == 200, r.text)


async def t_connect_failure_recovery(client, db_control):
    """建连失败后恢复：已有连接继续服务，数据库恢复后自动补足目标。

    用“拒绝新连接但保持旧会话可用”的方式模拟故障（PostgreSQL 的
    CONNECTION LIMIT 0），这样能精确验证“建连失败不影响在册连接”。
    """
    if db_control is None:
        print("[SKIP] connect-failure: no DB control command "
              "(set PSQL + DB_REJECT_CMD/DB_RESTORE_CMD)")
        return
    base = (await stats(client))["target"]
    await set_target(client, base)
    await wait_target(client, base)

    db_control("reject")
    try:
        before = await stats(client)
        # 在册连接仍然可用：普通查询继续成功，且不会新建连接
        ok = 0
        for _ in range(5):
            r = await client.post("/query/normal", json={"limit": 1})
            ok += r.status_code == 200
        check("connect-failure: existing conns still serve", ok == 5,
              f"ok={ok}")
        # 扩容触发后台建连；建连应失败，但目标与可用连接保留
        r = await client.post("/pool/target", json={"target": base + 2})
        check("connect-failure: set-target accepted while db rejects",
              r.status_code == 200, r.text)
        deadline = time.monotonic() + 20
        s = await stats(client)
        while (time.monotonic() < deadline
               and s["counters"]["connect_failures"]
               <= before["counters"]["connect_failures"]):
            await asyncio.sleep(0.2)
            s = await stats(client)
        check("connect-failure: failures observed",
              s["counters"]["connect_failures"]
              > before["counters"]["connect_failures"], str(s["counters"]))
        check("connect-failure: target not reached",
              not s["target_reached"] and s["opened_connections"] < base + 2,
              str(s))
        check("connect-failure: existing conns kept",
              s["opened_connections"] == base, str(s))
        # 建连失败期间已打开的连接照常服务
        r = await client.post("/query/normal", json={"limit": 1})
        check("connect-failure: service continues during failures",
              r.status_code == 200, r.text)
        r = await client.post("/query/slow", json={
            "delay_seconds": 0.3, "acquire_timeout": 5, "query_timeout": 5})
        check("connect-failure: slow query continues", r.status_code == 200,
              r.text)
    finally:
        db_control("restore")

    # 恢复后应自动补齐到 target
    s = await wait_target(client, base + 2, timeout=40)
    check("connect-failure: topped up after recovery",
          s["opened_connections"] == base + 2 and s["target_reached"], str(s))
    r = await client.post("/query/normal", json={"limit": 1})
    check("connect-failure: query works after recovery",
          r.status_code == 200, r.text)
    await set_target(client, base)
    s = await wait_target(client, base)
    check("connect-failure: shrink back to base", s["opened_connections"] == base,
          str(s))


async def t_shrink_while_connecting(client, db_control):
    """尚在建立的连接也占名额：建连期间缩容，建连完成后按新目标关闭。

    模拟“建连挂起”：暂停新连接的建立（DB_REJECT 会在建连阶段失败，
    此处用更直接的方式——把目标调大触发建连，随即调回）。
    """
    if db_control is None:
        print("[SKIP] shrink-while-connecting: no DB control command")
        return
    base = (await stats(client))["target"]
    await set_target(client, base)
    await wait_target(client, base)
    db_control("reject")
    try:
        # 扩容触发后台建连失败循环
        await client.post("/pool/target", json={"target": base + 2})
        await asyncio.sleep(1.2)
        mid = await stats(client)
        check("connecting: target above opened",
              mid["target"] == base + 2 and mid["opened_connections"] == base,
              str(mid))
        # 建连失败期间缩回：目标立即生效，不会等到恢复后才补
        r = await client.post("/pool/target", json={"target": base})
        check("connecting: shrink accepted", r.status_code == 200, r.text)
        s = await wait_target(client, base, timeout=20)
        check("connecting: target reached without new conns",
              s["opened_connections"] == base and s["target_reached"], str(s))
    finally:
        db_control("restore")
    await asyncio.sleep(1.5)  # 给后台一个重试周期，确认不会多开
    s = await stats(client)
    check("connecting: no extra conns opened after restore",
          s["opened_connections"] == base and s["connecting"] == 0, str(s))


# ---------------------------------------------------------------- 入口

async def main() -> int:
    timeout = httpx.Timeout(60.0)
    async with httpx.AsyncClient(base_url=BASE, timeout=timeout) as client:
        for _ in range(60):
            try:
                if (await client.get("/pool/stats")).status_code == 200:
                    break
            except Exception:  # noqa: BLE001
                await asyncio.sleep(1)
        else:
            print(f"API not reachable at {BASE}")
            return 2
        print(f"API up at {BASE}, pool target = {POOL_SIZE}, "
              f"psql checks: {'on' if PSQL else 'off'}, "
              f"db-control: {'on' if DB_REJECT_CMD and DB_RESTORE_CMD else 'off'}")
        await t_normal(client)
        await t_target_validation(client)
        await t_queue(client)
        await t_acquire_timeout(client)
        await t_queue_cancel(client)
        await t_query_timeout(client)
        await t_sql_error(client)
        await t_disconnect(client)
        await t_stress(client)
        await t_shrink_idle(client)
        await t_shrink_in_use(client)
        await t_shrink_grow_interleaved(client)
        control = db_control if DB_REJECT_CMD and DB_RESTORE_CMD else None
        await t_connect_failure_recovery(client, control)
        await t_shrink_while_connecting(client, control)
    print()
    if failures:
        print(f"{len(failures)} FAILURE(S):")
        for f in failures:
            print(" -", f)
        return 1
    print("ALL TESTS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
