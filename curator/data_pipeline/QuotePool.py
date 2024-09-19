import asyncio
import logging
import queue
import traceback
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Optional,
    Sequence,
)

from aiohttp import ClientSession, ClientTimeout, TCPConnector
from aiomultiprocess.core import Process
from aiomultiprocess.pool import CHILD_CONCURRENCY, MAX_TASKS_PER_CHILD, Pool, PoolResult, PoolWorker
from aiomultiprocess.scheduler import RoundRobin
from aiomultiprocess.types import (
    LoopInitializer,
    PoolTask,
    Queue,
    QueueID,
    R,
    T,
    TaskID,
)

log = logging.getLogger(__name__)


class QuoteScheduler(RoundRobin):
    """This scheduler is for use in the QuotePool for the Options Quotes downloader.
    It will make sure that all args for a given options ticker are put in the same queue
    Requires that all tasks add to the pool are ordered by option ticker.
    When the same option ticker is no longer"""

    def __init__(self, o_ticker_mapping: Dict[str, int]) -> None:
        super().__init__()
        self.current_o_ticker: str = ""
        self.current_queue: QueueID = 0
        self.o_ticker_mapping = o_ticker_mapping
        self.counter = 0
        self.queue_size: Dict[QueueID, int] = {}  # number of tasks in each queue

    def initialize_queue_size(self, args):
        """initialize the queue_size dict to count tasks per o_ticker.
        Also set initial current_o_ticker"""
        self.queue_size = {qid: 0 for qid in self.qids}
        self.current_o_ticker = args[0]

    def schedule_task(
        self,
        _task_id: TaskID,
        _func: Callable[..., Awaitable[R]],
        args: Sequence[Any],
        _kwargs: Dict[str, Any],
        pill: bool = False,
    ) -> QueueID:
        """required:args needs to have the OptionTicker tuple be the first arg in the tuple"""
        if not self.queue_size:
            self.initialize_queue_size(args)
        if pill:
            if self.o_ticker_mapping[self.current_o_ticker] == self.counter:
                self.queue_size[self.current_queue] += self.counter
            else:
                raise ValueError(
                    "incorrect number of tasks made for the o_ticker than were expected."
                    f"Expected: {self.o_ticker_mapping[self.current_o_ticker]}, Actual: {self.counter}"
                )
            self.counter = 0
            # self.current_o_ticker = clean_o_ticker(args[0])
            self.current_o_ticker = args[0]
            self.current_queue = self.cycle_queue()

        self.counter += 1
        return self.current_queue

    def cycle_queue(self) -> QueueID:
        """cycles the queue with the fewest tasks added to it"""

        return min(self.queue_size, key=self.queue_size.get)

    def complete_task(self, task_id):
        """removes number of tasks from queue_size dict based on o_ticker and self.o_ticker_mapping
        Unsure if it needs task_id, queue_id, o_ticker, or all of them"""
        # NOTE: as of now, no way to pass the o_ticker from the worker to the pool to the scheduler.
        # Don't want to pass results to the rx queue
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
            tx=tx,
            rx=rx,
            ttl=ttl,
            concurrency=concurrency,
            initializer=initializer,
            initargs=initargs,
            loop_initializer=loop_initializer,
            exception_handler=exception_handler,
            init_client_session=init_client_session,
            session_base_url=session_base_url,
        )
        self.o_ticker_count_mapping = o_ticker_count_mapping
        self.o_ticker_queue_progress: Dict[str, set[int]] = {}  # tids pulled to execute
        self.o_ticker_skip_tids: Dict[str, set[int]] = {}  # tids pulled to skip
        self.tid_result_progress: set = set()
        self.empty_tids: set = set()
        self.complete_otkrs: set[str] = set()
        self.completely_processed_otkrs: list[str] = []

    async def run(self):
        if self.init_client_session:
            async with ClientSession(
                connector=TCPConnector(limit_per_host=max(100, self.concurrency), use_dns_cache=True),
                timeout=ClientTimeout(total=90),
                base_url=self.session_base_url if self.session_base_url else None,
            ) as client_session:
                pending: Dict[asyncio.Future, TaskID] = {}
                completed: int = 0
                skipped: int = 0
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

                        # tracking progress
                        o_ticker = args[0]
                        if o_ticker not in self.o_ticker_queue_progress:
                            self.o_ticker_queue_progress[o_ticker] = set([tid])

                        # start work on task, add to pending if not a skipped/complete otkr.
                        # Otherwise add tid to "results" to mark as done
                        if o_ticker not in self.complete_otkrs:
                            self.o_ticker_queue_progress[o_ticker].add(tid)
                            args = [
                                *args,
                                client_session,
                            ]  # NOTE: adds client session to the args list
                            future = asyncio.ensure_future(func(*args, **kwargs))
                            pending[future] = tid
                        else:
                            self.rx.put_nowait((tid, None, None))
                            self.o_ticker_skip_tids[o_ticker].add(tid)
                            skipped += 1

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
                            if result:
                                if result[0] is False:
                                    if result[1] not in self.complete_otkrs:
                                        self.empty_tids.add(tid)
                            result = None
                        except BaseException as e:
                            if self.exception_handler is not None:
                                self.exception_handler(e)

                            tb = traceback.format_exc()
                        self.rx.put_nowait((tid, result, tb))
                        self.tid_result_progress.add(tid)
                        completed += 1

                    self.eval_list_date()

                    self.clean_o_ticker_progress()

        log.info(f"worker finished: processed {completed} tasks, and skipped {skipped}")

    def eval_list_date(self):
        k = 15  # indicator that we've passed the listing date for the option
        if len(self.empty_tids) > k:
            seq_start = self.has_consecutive_sequence(k=k)
            if seq_start:
                self.check_completed_otkr(seq_start)

    def has_consecutive_sequence(self, k=15) -> int | bool:
        """check if there is a sequence of length k or longer in which the tids are consecutive"""
        for tid in self.empty_tids:
            if all((tid + i) in self.empty_tids for i in range(k)):
                log.debug(f"consecutive sequence of {k} found within {len(self.empty_tids)} total empty tids")
                log.debug(f"empty tids: {self.empty_tids}")
                return tid
        return False

    def check_completed_otkr(self, tid):
        """lookup which oticker produced the consecutive sequence of empty tids"""
        now_empty_otkr = ""
        for otkr in self.o_ticker_queue_progress:
            if tid in self.o_ticker_queue_progress[otkr]:
                now_empty_otkr = otkr
                break
        if now_empty_otkr:
            self.complete_otkrs.add(now_empty_otkr)
            self.o_ticker_skip_tids[now_empty_otkr] = set()
            self.empty_tids -= self.o_ticker_queue_progress[now_empty_otkr]

    def clean_o_ticker_progress(self):
        """reports how many o_tickers have had all tids pulled from the queue and cleans internal tracking sets"""

        for otkr in self.complete_otkrs:
            total_tids = self.o_ticker_count_mapping[otkr]
            self.empty_tids -= self.o_ticker_queue_progress[otkr]
            if otkr not in self.completely_processed_otkrs:
                if (
                    len(self.o_ticker_skip_tids.get(otkr, []) | self.o_ticker_queue_progress.get(otkr, []))
                    >= total_tids
                ):
                    self.completely_processed_otkrs.append(otkr)
                    log.info(f"all processed for {otkr}! ({total_tids} tasks)")

                elif (
                    len(
                        (
                            self.o_ticker_queue_progress.get(otkr, set())
                            | self.o_ticker_skip_tids.get(otkr, set())
                        )
                        - self.tid_result_progress
                    )
                    == 0
                ):
                    self.completely_processed_otkrs.append(otkr)
                    log.info(f"all processed for {otkr}!! \
({len(self.o_ticker_queue_progress.get(otkr, []))} processed, \
{total_tids - len(self.o_ticker_queue_progress.get(otkr, []))} will be skipped, \
{total_tids} expected)")


class QuotePool(Pool):
    def __init__(
        self,
        processes: int = None,
        initializer: Callable[..., None] = None,
        initargs: Sequence[Any] = (),
        maxtasksperchild: int = MAX_TASKS_PER_CHILD,
        childconcurrency: int = CHILD_CONCURRENCY,
        queuecount: Optional[int] = None,
        scheduler: QuoteScheduler = QuoteScheduler,
        loop_initializer: Optional[LoopInitializer] = None,
        exception_handler: Optional[Callable[[BaseException], None]] = None,
        init_client_session: bool = False,
        session_base_url: Optional[str] = None,
        o_ticker_count_mapping: Dict[str, int] = None,
    ) -> None:
        self.o_ticker_count_mapping: dict[str, int] = o_ticker_count_mapping
        scheduler = QuoteScheduler(self.o_ticker_count_mapping)
        self.tasks_scheduled = 0
        super().__init__(
            processes=processes,
            initializer=initializer,
            initargs=initargs,
            maxtasksperchild=maxtasksperchild,
            childconcurrency=childconcurrency,
            queuecount=queuecount,
            scheduler=scheduler,
            loop_initializer=loop_initializer,
            exception_handler=exception_handler,
            init_client_session=init_client_session,
            session_base_url=session_base_url,
        )

    def queue_work(
        self,
        func: Callable[..., Awaitable[R]],
        args: Sequence[Any],
        kwargs: Dict[str, Any],
        pill: bool = False,
    ) -> TaskID:
        """
        pass the queues themselves to the scheduler enabling scheduling based on load.

        :meta private:
        """
        self.last_id += 1
        task_id = TaskID(self.last_id)

        qid = self.scheduler.schedule_task(
            task_id,
            func,
            args,
            kwargs,
            pill,
        )

        tx, _ = self.queues[qid]
        tx.put_nowait((task_id, func, args, kwargs))

        self.tasks_scheduled += 1
        if self.tasks_scheduled % 250000 == 0:
            log.debug(f"Tasks scheduled: {self.tasks_scheduled}")

        return task_id

    # def finish_work(self, task_id: TaskID, value: Any, tb: Optional[TracebackStr]):
    #     """overwriting the inherited function. Not using ._results in the pool"""
    #     self.scheduler.complete_task(task_id)

    def starmap(
        self,
        func: Callable[..., Awaitable[R]],
        iterable: Sequence[Sequence[T]],
    ):
        """Run a coroutine once for each sequence of items in the iterable."""
        if not self.running:
            raise RuntimeError("pool is closed")

        current_o_ticker = iterable[0][0]
        tids = []
        for args in iterable:
            pill = False
            if not args[0] == current_o_ticker:
                # passes pill to scheduler to cycle queues
                pill = True
                current_o_ticker = args[0]

            tid = self.queue_work(func, args, {}, pill=pill)
            tids.append(tid)
        return PoolResult(self, tids)

    def create_worker(
        self,
        qid: QueueID,
    ) -> Process:
        """
        Create a worker process attached to the given transmit and receive queues.

        :meta private:
        """
        tx, rx = self.queues[qid]
        process = QuoteWorker(
            tx,
            rx,
            self.childconcurrency,
            ttl=self.maxtasksperchild,
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
