import asyncio
import queue
import traceback
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
)

from aiohttp import ClientSession, ClientTimeout, TCPConnector
from aiomultiprocess.core import Process
from aiomultiprocess.pool import CHILD_CONCURRENCY, MAX_TASKS_PER_CHILD, Pool, PoolWorker
from aiomultiprocess.scheduler import RoundRobin
from aiomultiprocess.types import (
    LoopInitializer,
    PoolTask,
    Queue,
    QueueID,
    R,
    T,
    TaskID,
    TracebackStr,
)

from option_bot.data_pipeline.exceptions import PoolResultException
from option_bot.proj_constants import log
from option_bot.utils import clean_o_ticker


class QuoteScheduler(RoundRobin):
    """This scheduler is for use in the QuotePool for the Options Quotes downloader.
    It will make sure that all args for a given options ticker are put in the same queue
    Requires that all tasks add to the pool are ordered by option ticker.
    When the same option ticker is no longer"""

    def __init__(self, o_ticker_mapping: Dict[str, int]) -> None:
        super().__init__()
        self.current_o_ticker: str = ""
        self.current_queue: QueueID = 0
        self.queue_size: Dict[QueueID, int] = {}
        self.o_ticker_mapping = o_ticker_mapping
        self.counter = 0

    def schedule_task(
        self,
        queues: Dict[QueueID, Tuple[Queue, Queue]],
        _task_id: TaskID,
        _func: Callable[..., Awaitable[R]],
        args: Sequence[Any],
        _kwargs: Dict[str, Any],
        pill: bool = False,
    ) -> QueueID:
        """required:args needs to have the OptionTicker tuple be the first arg in the tuple"""
        self.counter += 1
        if pill:
            self.queue_size[self.current_queue] += self.counter
            self.counter = 0
            self.current_o_ticker = ""
        elif clean_o_ticker(args[0]) != self.current_o_ticker:
            self.current_o_ticker = clean_o_ticker(args[0])
            self.current_queue = self.cycle_queue(queues)
        return self.current_queue

    def cycle_queue(self, queues: Dict[QueueID, Tuple[Queue, Queue]]) -> QueueID:
        """cycles the queue with the fewest tasks added to it"""

        return min(self.queue_size, key=self.queue_size.get)

    def complete_task(self, task_id):
        """removes number of tasks from queue_size dict based on o_ticker and self.o_ticker_mapping
        Unsure if it needs task_id, queue_id, o_ticker, or all of them"""
        # NOTE: as of now, no way to pass the o_ticker from the worker to the pool to the scheduler.
        pass


class QuoteWorker(PoolWorker):
    """this worker is meant for the processing of quote queues.
    The TTL should be triggered once the tasks in the queue switch o_tickers.
    Thereby, a worker should write the results to disc and then die
    with a new one spinning up to process the next o_ticker"""

    def __init__(
        self,
        tx: Queue,
        rx: Queue,
        concurrency: int = CHILD_CONCURRENCY,
        ttl: int = MAX_TASKS_PER_CHILD,
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
        self._results: Dict[TaskID, Tuple[Any, Optional[TracebackStr]]] = {}

    def clean_up_queue(self, completed: int):
        wont_do = self.o_ticker_count_mapping[self.o_ticker] - completed
        if wont_do > 0:
            log.info(f"cleaning up {wont_do} tasks from the queue")
            for _ in range(wont_do):
                self.tx.get_nowait()

    def save_results(self, func: Callable):
        """save the results to disc"""
        results = [x[0] for x in self._results.values() if x[0] is not None]
        func(results, self.o_ticker, self.underlying_ticker)

    async def run(self):
        if self.init_client_session:
            async with ClientSession(
                connector=TCPConnector(limit_per_host=max(100, self.concurrency), use_dns_cache=True),
                timeout=ClientTimeout(total=90),
                base_url=self.session_base_url if self.session_base_url else None,
            ) as client_session:
                pending: Dict[asyncio.Future, TaskID] = {}
                pulled_tids: List[TaskID] = []
                completed: int = 0
                empty_tids: List[TaskID] = []
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
                        pulled_tids.append(tid)

                        # set worker o_ticker value
                        if self.o_ticker == "":
                            self.o_ticker = args[0]
                            self.underlying_ticker = clean_o_ticker(self.o_ticker)
                            self.ttl = self.o_ticker_count_mapping[self.o_ticker]

                        # start work on task, add to pending
                        if args[0] == self.o_ticker:
                            args = [
                                *args,
                                client_session,
                            ]  # NOTE: adds client session to the args list
                            future = asyncio.ensure_future(func(*args, **kwargs))
                            pending[future] = tid

                        else:
                            raise PoolResultException(
                                "Quote Worker can't working on more than one o_ticker",
                                extra={
                                    "context": f"trying {args[0]} while current o_ticker is {self.o_ticker}"
                                },
                            )

                        # poison pill 1: all o_ticker args have been processed
                        if args[0] is None and args[1] is None:
                            running = False
                            break

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

                        completed += 1
                        if result is not None:
                            if len(result.get("results")) > 0:
                                self._results[tid] = (result.get("results"), tb)
                            else:
                                empty_tids.append(tid)

                    # poison pill 2: o_ticker args are returning no results
                    if self.has_consecutive_sequence(empty_tids):
                        running = False

                self.save_results()
                if completed != len(pulled_tids):
                    log.warning("completed != pulled_tids...check why or you can trust the queue cleaner")

                self.clean_up_queue(completed)

    def has_consecutive_sequence(self, tids: List[TaskID], k=16) -> bool:
        """check if there is a sequence of length 16 or longer in which the tids are consecutive"""
        num_set = set(tids)
        for tid in tids:
            if all((tid + 1) in num_set for i in range(k)):
                return True
        return False


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
        self.o_ticker_count_mapping: dict[str, int] = o_ticker_count_mapping
        scheduler = QuoteScheduler(self.o_ticker_count_mapping)
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

    def queue_work(
        self,
        func: Callable[..., Awaitable[R]],
        args: Sequence[Any],
        kwargs: Dict[str, Any],
        pill: bool = False,
    ):
        """
        pass the queues themselves to the scheduler enabling scheduling based on load.

        :meta private:
        """
        self.last_id += 1
        task_id = TaskID(self.last_id)

        qid = self.scheduler.schedule_task(
            self.queues,
            task_id,
            func,
            args,
            kwargs,
            pill,
        )
        tx, _ = self.queues[qid]
        tx.put_nowait((task_id, func, args, kwargs))

    async def results(self, tids):
        """overwrites the inherited results so it's not called accidentally"""
        raise PoolResultException("no results stored in QuotePool, check QuoteWorker instead")

    async def loop(self) -> None:
        """
        Maintain the pool of workers while open.

        :meta private:
        """
        while self.processes or self.running:
            # clean up workers that reached TTL
            for process in list(self.processes):
                if not process.is_alive():
                    qid = self.processes.pop(process)
                    if self.running:
                        self.processes[self.create_worker(qid)] = qid
            # let someone else do some work for once
            await asyncio.sleep(0.005)

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
                self.queue_work(func_2, (None, None), {}, pill=True)
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
