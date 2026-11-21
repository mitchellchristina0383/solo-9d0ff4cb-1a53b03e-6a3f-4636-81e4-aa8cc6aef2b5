"""一个语义显式的异步 PostgreSQL 连接池。

设计目标（详见 README）：

- **目标连接数可在线调整**：每条连接始终属于一个“槽位”（idle /
  in_use / cleaning / connecting 之一）。缩容只关闭多余的空闲连接，
  已借出连接的查询照常跑完；打开数仍高于目标期间暂停新借出和补开；
  建连与脏连接清理结束后，按最新接受的目标决定留用还是关闭。
- 可能仍在执行查询的连接（客户端超时、断开）或发生过 SQL 错误的
  连接，在确认会话状态已清理之前绝不会被再次借出；无法确认的一律
  关闭并由后台补齐；
- 建连失败不影响可用连接继续服务，后台按退避间隔重试，数据库恢复
  后自动补足到目标；
- 排队中的请求不占用连接名额，被取消后立即离开等待队列。
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from contextlib import suppress
from dataclasses import dataclass

import psycopg
from psycopg.pq import TransactionStatus

logger = logging.getLogger("pool")


class AcquireTimeoutError(Exception):
    """在请求的超时时间内没有可用连接。"""


class PoolClosedError(Exception):
    """连接池已关闭。"""


def _consume_task_exception(task: asyncio.Task) -> None:
    """取回被我们有意放弃的任务的异常，避免事件循环告警。"""
    if task.cancelled():
        return
    try:
        task.exception()
    except Exception:
        pass


@dataclass
class PoolConfig:
    dsn: dict
    size: int = 4                 # 初始目标连接数（之后可在运行时调整）
    connect_timeout: float = 5.0
    startup_retries: int = 60      # 启动时等待数据库就绪的重试次数
    retry_interval: float = 1.0    # 后台建连失败后的首次退避间隔
    max_retry_interval: float = 5.0
    cleanup_timeout: float = 10.0  # 清理单条连接的总预算
    drain_grace: float = 5.0       # 等待被取消的查询收尾的时间
    close_timeout: float = 3.0
    # 空闲超过该秒数的连接，借出前先用 SELECT 1 验证活性
    # （防止数据库重启后把已死的空闲连接借给请求）；None 表示从不验证
    checkout_verify_after: float | None = 10.0
    checkout_verify_timeout: float = 3.0

    def __post_init__(self) -> None:
        if (isinstance(self.size, bool) or not isinstance(self.size, int)
                or self.size < 1):
            raise ValueError("size must be a positive integer")


class AsyncPool:
    def __init__(self, config: PoolConfig):
        self.cfg = config
        self._cond = asyncio.Condition()
        # 空闲连接及其进入空闲的时刻（monotonic），用于借出前活性验证
        self._idle: list[tuple[psycopg.AsyncConnection, float]] = []
        self._in_use = 0    # 已借给请求
        self._cleaning = 0  # 正在清理/验证（仍占用打开名额，不可借出）
        self._connecting = 0  # 正在建立连接（尚未打开，但占名额）
        self._waiting = 0   # 正在 acquire() 中排队的请求数
        self._opened = 0    # 当前打开的连接数（idle + in_use + cleaning）
        self._target = config.size  # 最新接受的目标连接数
        self._closed = False
        self._bg: set[asyncio.Task] = set()
        self._manager: asyncio.Task | None = None
        # 正在清理的连接对象，关停时用于兜底关闭被取消的清理任务
        self._cleaning_conns: set[psycopg.AsyncConnection] = set()
        self.counters: dict[str, int] = {
            "acquired": 0,           # 成功借出次数
            "acquire_timeouts": 0,   # 等连接超时次数
            "queue_cancelled": 0,    # 排队中客户端取消次数
            "released_clean": 0,     # 干净归还次数
            "recycled": 0,           # 脏连接清理后复用次数
            "replaced": 0,           # 脏连接废弃次数（之后由后台按目标补齐）
            "server_cancels": 0,     # 发送的服务端取消次数
            "query_timeouts": 0,     # 查询执行超时次数
            "client_disconnects": 0,  # 查询期间客户端断开次数
            "sql_errors": 0,         # SQL 报错次数
            "resizes": 0,            # 接受的目标连接数调整次数
            "closed_for_resize": 0,  # 因缩容而关闭的连接数
            "opened_total": 0,       # 累计成功建立的连接数
            "connect_failures": 0,   # 后台建连失败次数
        }

    # ------------------------------------------------------------ 生命周期

    async def start(self) -> None:
        # 初始按 cfg.size 建满；startup_retries 用于等待数据库就绪。
        for _ in range(self.cfg.size):
            conn = await self._connect_with_retry(self.cfg.startup_retries)
            async with self._cond:
                self._opened += 1
                self.counters["opened_total"] += 1
                self._idle.append((conn, time.monotonic()))
        # 负责扩容 / 故障恢复后补足目标的后台任务
        self._manager = self.spawn(self._manage())
        logger.info("pool started with %d connections", len(self._idle))

    async def close(self) -> None:
        async with self._cond:
            if self._closed:
                return
            self._closed = True
            idle, self._idle = self._idle, []
            self._opened -= len(idle)
            cleaning = list(self._cleaning_conns)
            self._cleaning_conns.clear()
            self._cond.notify_all()
        if self._manager is not None:
            self._manager.cancel()
            with suppress(asyncio.CancelledError):
                await self._manager
        tasks = [t for t in list(self._bg) if t is not self._manager]
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task
        for conn, _ in idle:
            await self._close_quietly(conn)
        for conn in cleaning:
            await self._close_quietly(conn)

    # ------------------------------------------------------- 目标数调整

    async def set_target(self, target: int) -> dict:
        """接受新的目标连接数（正整数），返回调整后的指标快照。

        - 缩容：立即关闭多余的空闲连接；已借出的连接等查询跑完再关闭；
        - 打开数仍高于目标期间，acquire() 暂停新借出，后台也不补开；
        - 扩容 / 补开由后台任务异步完成（建连失败会退避重试），
          调用方可通过 /pool/stats 的 `target_reached` 判断是否已达成。
        - 对同一目标重复调用是幂等的（no-op）。
        """
        if (isinstance(target, bool) or not isinstance(target, int)
                or target < 1):
            raise ValueError("target must be a positive integer")
        closing: list[psycopg.AsyncConnection] = []
        async with self._cond:
            if self._closed:
                raise PoolClosedError("pool is closed")
            previous = self._target
            self._target = target
            if target != previous:
                self.counters["resizes"] += 1
            # 缩容优先关闭多余的空闲连接
            surplus = self._opened - target
            while surplus > 0 and self._idle:
                conn, _ = self._idle.pop()
                closing.append(conn)
                self._opened -= 1
                self.counters["closed_for_resize"] += 1
                surplus -= 1
            self._cond.notify_all()
        # 先把空闲连接真正关掉再返回：调用方（如值班员）可以立即指望
        # 这些会话已在服务端释放。关闭失败也有 close_timeout 兜底。
        for conn in closing:
            await self._close_quietly(conn)
        if target != previous or closing:
            logger.info("pool target %d -> %d (closed %d idle)",
                        previous, target, len(closing))
        result = await self.stats()
        result["accepted"] = True
        result["previous_target"] = previous
        return result

    # -------------------------------------------------------- 借出 / 归还

    async def acquire(self, timeout: float | None) -> psycopg.AsyncConnection:
        """从池中取一条连接。

        在 `timeout` 秒内没有可用连接则抛出 AcquireTimeoutError
        （None 表示无限等待）。排队中的协程被取消时，其队列名额立即
        释放——排队请求从不持有连接名额。

        缩容期间（打开数仍高于目标）暂停借出：即使有连接刚被归还，
        也会优先关闭以收缩到目标，新的请求在目标达成后才会被唤醒。

        借出前如发现连接已死，或空闲时间过长且 SELECT 1 验证失败，
        则将其转入清理/更换并继续等下一条，不会把坏连接交给调用方。
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            conn, idle_since = await self._take_idle(deadline)
            if await self._verify_checkout(conn, idle_since):
                return conn
            # 验证失败：连接已按脏连接送清理/更换，在剩余预算内重试
            if deadline is not None and time.monotonic() >= deadline:
                self.counters["acquire_timeouts"] += 1
                raise AcquireTimeoutError(
                    f"no connection available within {timeout}s")

    async def _take_idle(
        self, deadline: float | None
    ) -> tuple[psycopg.AsyncConnection, float]:
        async with self._cond:
            self._waiting += 1
            try:
                while True:
                    if self._closed:
                        raise PoolClosedError("pool is closed")
                    # 打开数高于目标 = 仍在缩容：暂停新借出，先收缩。
                    if self._idle and self._opened <= self._target:
                        conn, idle_since = self._idle.pop()
                        self._in_use += 1
                        self.counters["acquired"] += 1
                        return conn, idle_since
                    if deadline is None:
                        await self._cond.wait()
                        continue
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        self.counters["acquire_timeouts"] += 1
                        raise AcquireTimeoutError("no connection available")
                    try:
                        await asyncio.wait_for(self._cond.wait(), remaining)
                    except asyncio.TimeoutError:
                        self.counters["acquire_timeouts"] += 1
                        raise AcquireTimeoutError(
                            "no connection available") from None
            finally:
                self._waiting -= 1

    async def _verify_checkout(self, conn, idle_since: float) -> bool:
        """借出前的最后把关。本地状态免费检查总是做；空闲太久的连接
        额外做一次 SELECT 1 活性验证。"""
        if not self._looks_clean(conn):
            await self.release(conn, dirty=True)
            return False
        after = self.cfg.checkout_verify_after
        if after is None or time.monotonic() - idle_since < after:
            return True
        try:
            async with conn.cursor() as cur:
                await asyncio.wait_for(
                    cur.execute("SELECT 1"), self.cfg.checkout_verify_timeout)
        except Exception:
            logger.info("checkout verify failed; recycling connection")
            await self.release(conn, dirty=True)
            return False
        if self._looks_clean(conn):
            return True
        await self.release(conn, dirty=True)
        return False

    async def release(
        self,
        conn: psycopg.AsyncConnection,
        *,
        dirty: bool,
        pending: asyncio.Task | None = None,
    ) -> None:
        """归还 acquire() 借出的连接。

        干净的连接在目标之内直接回到空闲队列；若池正在缩容（打开数
        高于目标），则直接关闭，不再出借。脏连接（查询超时 / 客户端
        断开 / SQL 报错）转入后台清理，只有在会话状态被验证干净后才可
        再次借出，无法验证的关闭并由后台按目标补齐。
        """
        if pending is not None:
            pending.add_done_callback(_consume_task_exception)
        if not dirty and self._looks_clean(conn):
            async with self._cond:
                self._in_use -= 1
                if self._closed or self._opened > self._target:
                    self._opened -= 1
                    if not self._closed:
                        self.counters["closed_for_resize"] += 1
                    drop = True
                else:
                    self._idle.append((conn, time.monotonic()))
                    self.counters["released_clean"] += 1
                    drop = False
                self._cond.notify_all()
            if drop:
                await self._close_quietly(conn)
            return
        async with self._cond:
            self._in_use -= 1
            if self._closed:
                # 关停中：直接关闭，不再排后台清理
                self._opened -= 1
                drop = True
            else:
                self._cleaning += 1
                self._cleaning_conns.add(conn)
                drop = False
            self._cond.notify_all()
        if drop:
            await self._close_quietly(conn)
            return
        self.spawn(self._recycle(conn, pending))

    # ------------------------------------------------------------ 内部实现

    def _looks_clean(self, conn: psycopg.AsyncConnection) -> bool:
        try:
            return (
                not conn.closed
                and not getattr(conn, "broken", False)
                and conn.info.transaction_status == TransactionStatus.IDLE
            )
        except Exception:
            return False

    def spawn(self, coro) -> asyncio.Task:
        """在池的后台任务集中运行一个协程（供清理与建连任务使用）。"""
        task = asyncio.create_task(coro)
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)
        task.add_done_callback(_consume_task_exception)
        return task

    async def _recycle(self, conn, pending) -> None:
        """清理一条脏连接，并按“最新接受的目标”决定留用或关闭。"""
        try:
            ok = await self._sanitize(conn, pending)
        except asyncio.CancelledError:
            # 关停流程会负责关闭仍登记在 _cleaning_conns 里的连接
            raise
        except Exception:
            logger.exception("sanitize crashed")
            ok = False
        async with self._cond:
            self._cleaning -= 1
            keep = ok and not self._closed and self._opened <= self._target
            if keep:
                self._cleaning_conns.discard(conn)
                self._idle.append((conn, time.monotonic()))
                self.counters["recycled"] += 1
            else:
                self._opened -= 1
                if not ok:
                    self.counters["replaced"] += 1
                elif not self._closed:
                    # 清理完成时目标已被调低，按最新目标关闭
                    self.counters["closed_for_resize"] += 1
            self._cond.notify_all()
        if not keep:
            if ok:
                logger.info("cleaned connection no longer needed; closing")
            else:
                logger.info("connection discarded; will top up if needed")
            await self._close_quietly(conn)
            async with self._cond:
                # 关闭后才摘除登记：关停流程不会漏掉正在关闭的连接
                self._cleaning_conns.discard(conn)

    async def _sanitize(self, conn, pending) -> bool:
        """确认连接的会话状态已清理才返回 True。"""
        if conn.closed or getattr(conn, "broken", False):
            return False
        try:
            return await asyncio.wait_for(
                self._sanitize_inner(conn, pending), self.cfg.cleanup_timeout
            )
        except Exception:
            logger.warning("sanitize failed", exc_info=True)
            return False

    async def _sanitize_inner(self, conn, pending) -> bool:
        # 1) 查询可能仍在服务端执行：发送服务端取消，并等待客户端侧的
        #    查询任务观察到取消结果。
        still_running = pending is not None and not pending.done()
        if still_running or conn.info.transaction_status == TransactionStatus.ACTIVE:
            await self._server_cancel(conn)
        if still_running:
            try:
                await asyncio.wait_for(asyncio.shield(pending), self.cfg.drain_grace)
            except Exception:
                pass
            if not pending.done():
                return False  # 查询不肯结束 -> 无法信任该会话
        # 2) 回滚任何遗留 / 失败的事务。
        status = conn.info.transaction_status
        if status in (TransactionStatus.INTRANS, TransactionStatus.INERROR):
            await conn.rollback()
        # 3) 验证会话可用且处于空闲。
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1")
            row = await cur.fetchone()
        return (
            row is not None
            and tuple(row) == (1,)
            and conn.info.transaction_status == TransactionStatus.IDLE
        )

    async def _server_cancel(self, conn) -> None:
        try:
            result = conn.cancel()
            if inspect.isawaitable(result):  # 兼容不同 psycopg 版本
                await result
            self.counters["server_cancels"] += 1
        except Exception:
            logger.warning("server-side cancel failed", exc_info=True)

    async def _close_quietly(self, conn) -> None:
        try:
            await asyncio.wait_for(conn.close(), self.cfg.close_timeout)
        except Exception:
            logger.warning("error while closing connection", exc_info=True)

    async def _connect(self) -> psycopg.AsyncConnection:
        return await psycopg.AsyncConnection.connect(
            **self.cfg.dsn,
            autocommit=True,
            application_name="ro-api",
            connect_timeout=int(self.cfg.connect_timeout),
            # 会话级只读：本 API 是只读查询服务
            options="-c default_transaction_read_only=on",
        )

    async def _connect_with_retry(self, retries: int) -> psycopg.AsyncConnection:
        """retries <= 0 表示一直重试。"""
        attempt = 0
        while True:
            try:
                return await self._connect()
            except Exception:
                attempt += 1
                if 0 < retries <= attempt:
                    raise
                logger.warning("connect failed (attempt %d), retrying", attempt)
                await asyncio.sleep(self.cfg.retry_interval)

    def _needs_conn(self) -> bool:
        """是否需要补开：打开的 + 正在建立的仍少于目标（调用方持锁）。"""
        return self._opened + self._connecting < self._target

    async def _manage(self) -> None:
        """后台维持目标连接数：扩容、故障恢复后补足。

        串行建连（一次最多一条在途），失败的连接只影响“补开”进度，
        不阻塞已打开连接的服务；失败后按指数退避重试，直到达成目标
        或连接池关闭。建连完成时按“最新接受的目标”决定留用或关闭。
        """
        backoff = self.cfg.retry_interval
        while True:
            async with self._cond:
                await self._cond.wait_for(
                    lambda: self._closed or self._needs_conn())
                if self._closed:
                    return
                self._connecting += 1
            try:
                conn = await self._connect()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.counters["connect_failures"] += 1
                async with self._cond:
                    self._connecting -= 1
                    closed = self._closed
                    self._cond.notify_all()
                logger.warning("background connect failed: %s", exc)
                if closed:
                    return
                # 退避；期间池关闭会取消本任务
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.cfg.max_retry_interval)
                continue
            backoff = self.cfg.retry_interval
            self.counters["opened_total"] += 1
            async with self._cond:
                self._connecting -= 1
                # 以最新目标为准：建连期间目标可能已被调低
                keep = (not self._closed) and self._opened < self._target
                if keep:
                    self._opened += 1
                    self._idle.append((conn, time.monotonic()))
                self._cond.notify_all()
            if not keep:
                logger.info("connection finished after shrink; closing it")
                await self._close_quietly(conn)

    # ------------------------------------------------------------ 指标

    async def stats(self) -> dict:
        async with self._cond:
            return {
                # size 保留为 target 的别名，兼容旧调用方
                "size": self._target,
                "target": self._target,
                "idle": len(self._idle),
                "in_use": self._in_use,
                "cleaning": self._cleaning,
                "connecting": self._connecting,
                "waiting": self._waiting,
                "opened_connections": self._opened,
                "target_reached": (
                    self._opened == self._target and self._connecting == 0),
                "counters": dict(self.counters),
            }
