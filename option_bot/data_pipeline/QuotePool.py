import asyncio
import queue
import traceback
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Generator,
    Optional,
    Sequence,
    Tuple,
)

from aiohttp import ClientSession, ClientTimeout, TCPConnector
from aiomultiprocess.core import Process
from aiomultiprocess.pool import _T, CHILD_CONCURRENCY, MAX_TASKS_PER_CHILD, Pool, PoolWorker
from aiomultiprocess.scheduler import RoundRobin
from aiomultiprocess.types import LoopInitializer, PoolTask, ProxyException, Queue, QueueID, R, T, TaskID

from option_bot.data_pipeline.exceptions import PoolResultException
from option_bot.utils import clean_o_ticker

# from option_bot.utils import extract_underlying_from_o_ticker


class QuoteScheduler(RoundRobin):
    """This scheduler is for use in the QuotePool for the Options Quotes downloader.
    It will make sure that all args for a given options ticker are put in the same queue
    Requires that all tasks add to the pool are ordered by option ticker.
    When the same option ticker is no longer"""

    def __init__(self) -> None:
        super().__init__()
        self.current_o_ticker: str = ""
        self.current_queue: QueueID = 0

    def schedule_task(
        self,
        queues: Dict[QueueID, Tuple[Queue, Queue]],
        _task_id: TaskID,
        _func: Callable[..., Awaitable[R]],
        args: Sequence[Any],
        _kwargs: Dict[str, Any],
    ) -> QueueID:
        """required:args needs to have the OptionTicker tuple be the first arg in the tuple"""
        if self.get_o_ticker_from_url(args[0]) != self.current_o_ticker:
            self.current_o_ticker = self.get_o_ticker_from_url(args[0])
            self.current_queue = self.cycle_queue(queues)
        return self.current_queue

    def cycle_queue(self, queues: Dict[QueueID, Tuple[Queue, Queue]]) -> QueueID:
        """cycles the queue with the fewest tasks"""
        queue_vals = {qid: len(queues[qid][0]) for qid in self.qids}
        return min(queue_vals, key=queue_vals.get)

    @staticmethod
    def get_o_ticker_from_url(url: str) -> str:
        return clean_o_ticker(url)


class QuoteWorker(PoolWorker):
    """this worker is meant for the processing of quote queues.
    The TTL should be triggered once the tasks in the queue switch o_tickers.
    Thereby, a worker should write the results to disc and then die
    with a new one spinning up to process the next o_ticker"""

    def __init__(
        self,
        tx: Queue,
        rx: Queue,
        ttl: int = MAX_TASKS_PER_CHILD,
        concurrency: int = CHILD_CONCURRENCY,
        *,
        initializer: Optional[Callable] = None,
        initargs: Sequence[Any] = (),
        loop_initializer: Optional[LoopInitializer] = None,
        exception_handler: Optional[Callable[[BaseException], None]] = None,
        init_client_session: bool = False,
        session_base_url: Optional[str] = None,
        o_ticker_count_mapping: Dict[str, int] = None,
    ) -> None:
        super().__init__(
            tx,
            rx,
            ttl,
            concurrency,
            initializer,
            initargs,
            loop_initializer,
            exception_handler,
            init_client_session,
            session_base_url,
        )
        self.o_ticker_count_mapping = o_ticker_count_mapping
        self.o_ticker: str = ""
        self.underlying_ticker: str = ""
        self._results: Dict[TaskID, Tuple[R, Optional[str]]] = {}

    def clean_up_queue(self):
        pass

    def save_results(self):
        pass

    async def run(self):
        if self.init_client_session:
            async with ClientSession(
                connector=TCPConnector(limit_per_host=max(100, self.concurrency), use_dns_cache=True),
                timeout=ClientTimeout(total=90),
                base_url=self.session_base_url if self.session_base_url else None,
            ) as client_session:
                pending: Dict[asyncio.Future, TaskID] = {}
                completed = 0
                running = True
                while running or pending:
                    # TTL, Tasks To Live, determines how many tasks to execute before dying
                    if self.ttl and completed >= self.ttl:
                        running = False

                    # pick up new work as long as we're "running" and we have open slots
                    while running and len(pending) < self.concurrency:
                        try:
                            task: PoolTask = self.tx.get_nowait()
                        except queue.Empty:
                            break

                        if task is None:
                            running = False
                            break

                        tid, func, args, kwargs = task

                        args = [
                            *args,
                            client_session,
                        ]  # NOTE: adds client session to the args list
                        future = asyncio.ensure_future(func(*args, **kwargs))
                        pending[future] = tid

                    if not pending:
                        await asyncio.sleep(0.005)
                        continue

                    # return results and/or exceptions when completed
                    done, _ = await asyncio.wait(
                        pending.keys(),
                        timeout=0.05,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for future in done:
                        tid = pending.pop(future)

                        result = None
                        tb = None
                        try:
                            result = future.result()
                        except BaseException as e:
                            if self.exception_handler is not None:
                                self.exception_handler(e)

                            tb = traceback.format_exc()

                        self.rx.put_nowait((tid, result, tb))
                        completed += 1

    async def results(self, tids: Sequence[TaskID]) -> Sequence[R]:
        """
        Wait for all tasks to complete, and return results, preserving order.

        :meta private:
        """
        pending = set(tids)
        ready: Dict[TaskID, R] = {}

        while pending:
            for tid in pending.copy():
                if tid in self._results:
                    result, tb = self._results.pop(tid)
                    if tb is not None:
                        raise ProxyException(tb)
                    ready[tid] = result
                    pending.remove(tid)

            await asyncio.sleep(0.005)

        return [ready[tid] for tid in tids]


class QuoteWorkerResult(Awaitable[Sequence[_T]], AsyncIterable[_T]):
    """
    Asynchronous proxy for map/starmap results. Can be awaited or used with `async for`.
    """

    def __init__(self, worker: "QuoteWorker", task_ids: Sequence[TaskID]):
        self.worker = worker
        self.task_ids = task_ids

    def __await__(self) -> Generator[Any, None, Sequence[_T]]:
        """Wait for all results and return them as a sequence"""
        return self.results().__await__()

    async def results(self) -> Sequence[_T]:
        """Wait for all results and return them as a sequence"""
        return await self.worker.results(self.task_ids)

    def __aiter__(self) -> AsyncIterator[_T]:
        """Return results one-by-one as they are ready"""
        return self.results_generator()

    async def results_generator(self) -> AsyncIterator[_T]:
        """Return results one-by-one as they are ready"""
        for task_id in self.task_ids:
            yield (await self.worker.results([task_id]))[0]


class QuotePool(Pool):
    def __init__(
        self,
        processes: int = None,
        initializer: Callable[..., None] = None,
        initargs: Sequence[Any] = (),
        maxtasksperchild: int = MAX_TASKS_PER_CHILD,
        childconcurrency: int = CHILD_CONCURRENCY,
        queuecount: Optional[int] = None,
        scheduler: QuoteScheduler = QuoteScheduler(),
        loop_initializer: Optional[LoopInitializer] = None,
        exception_handler: Optional[Callable[[BaseException], None]] = None,
        init_client_session: bool = False,
        session_base_url: Optional[str] = None,
        o_ticker_count_mapping: Dict[str, int] = None,
    ) -> None:
        super().__init__(
            processes,
            initializer,
            initargs,
            maxtasksperchild,
            childconcurrency,
            queuecount,
            scheduler,
            loop_initializer,
            exception_handler,
            init_client_session,
            session_base_url,
        )
        self.o_ticker_count_mapping: dict[str, int] = o_ticker_count_mapping

    def queue_work(
        self,
        func: Callable[..., Awaitable[R]],
        args: Sequence[Any],
        kwargs: Dict[str, Any],
    ):
        """
        pass the queues themselves to the scheduler enabling scheduling based on load.

        :meta private:
        """
        self.last_id += 1
        task_id = TaskID(self.last_id)

        qid = self.scheduler.schedule_task(self.queues, task_id, func, args, kwargs)
        tx, _ = self.queues[qid]
        tx.put_nowait((task_id, func, args, kwargs))

    async def results(self, tids):
        """overwrites the inherited results so it's not called accidentally"""
        raise PoolResultException("no results stored in QuotePool, check QuoteWorker instead")

    def starmap(
        self,
        func: Callable[..., Awaitable[R]],
        func_2: Callable[..., Awaitable[R]],
        iterable: Sequence[Sequence[T]],
    ):
        """Run a coroutine once for each sequence of items in the iterable."""
        if not self.running:
            raise RuntimeError("pool is closed")

        current_o_ticker = iterable[0][0]
        for args in iterable:
            if args[0] == current_o_ticker:
                self.queue_work(func, args, {})
            else:
                # passes poison pill to worker to save results and die
                self.queue_work(func_2, (None, None), {})
                current_o_ticker = args[0]


def create_worker(self, qid: QueueID, o_ticker: str, ttl: int) -> Process:
    """
    Create a worker process attached to the given transmit and receive queues.

    :meta private:
    """
    tx, rx = self.queues[qid]
    process = QuoteWorker(
        tx,
        rx,
        self.childconcurrency,
        initializer=self.initializer,
        initargs=self.initargs,
        loop_initializer=self.loop_initializer,
        exception_handler=self.exception_handler,
        init_client_session=self.init_client_session,
        session_base_url=self.session_base_url,
        o_ticker_count_mapping=self.o_ticker_count_mapping,
    )
    process.start()
    return process


async def loop(self) -> None:
    """
    Maintain the pool of workers while open. Map workers to o_tickers.

    :meta private:
    """
    while self.processes or self.running:
        # clean up workers that reached TTL
        for process in list(self.processes):
            if not process.is_alive():
                qid = self.processes.pop(process)
                if self.running:
                    self.processes[self.create_worker(qid)] = qid

        # pull results into a shared dictionary for later retrieval
        for _, rx in self.queues.values():
            while True:
                try:
                    task_id, value, tb = rx.get_nowait()
                    self.finish_work(task_id, value, tb)

                except queue.Empty:
                    break

        # let someone else do some work for once
        await asyncio.sleep(0.005)
