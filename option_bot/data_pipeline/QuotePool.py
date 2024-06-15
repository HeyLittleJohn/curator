from typing import Any, Awaitable, Callable, Dict, Optional, Sequence, Tuple

from aiomultiprocess.pool import CHILD_CONCURRENCY, MAX_TASKS_PER_CHILD, Pool, PoolWorker
from aiomultiprocess.scheduler import RoundRobin
from aiomultiprocess.types import LoopInitializer, Queue, QueueID, R, TaskID
from db_tools.utils import OptionTicker


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
        self.current_o_ticker: OptionTicker = None


class QuotePool(Pool):
    def __init__(self) -> None:
        super().__init__()

    def queue_work(
        self,
        func: Callable[..., Awaitable[R]],
        args: Sequence[Any],
        kwargs: Dict[str, Any],
    ) -> TaskID:
        """
        Add a new work item to the outgoing queue.

        :meta private:
        """
        self.last_id += 1
        task_id = TaskID(self.last_id)

        qid = self.scheduler.schedule_task(self.queues, task_id, func, args, kwargs)
        tx, _ = self.queues[qid]
        tx.put_nowait((task_id, func, args, kwargs))
        return task_id


class OTickerScheduler(RoundRobin):
    """This scheduler is for use in the QuotePool for the Options Quotes downloader.
    It will make sure that all args for a given options ticker are put in the same queue
    Requires that all tasks add to the pool are ordered by option ticker.
    When the same option ticker is no longer"""

    def __init__(self) -> None:
        super().__init__()
        self.current_o_ticker: OptionTicker = None
        self.current_queue: QueueID = self.qids[0]

    def schedule_task(
        self,
        queues: Dict[QueueID, Tuple[Queue, Queue]],
        _task_id: TaskID,
        _func: Callable[..., Awaitable[R]],
        args: Sequence[Any],
        _kwargs: Dict[str, Any],
    ) -> QueueID:
        """required:args needs to have the OptionTicker tuple be the first arg in the tuple"""
        if args[0].option_ticker != self.current_o_ticker:
            self.current_o_ticker = args[0].option_ticker
            self.current_queue = self.cycle_queue(queues)
        return self.current_queue

    def cycle_queue(self, queues: Dict[QueueID, Tuple[Queue, Queue]]) -> QueueID:
        """cycles the queue with the fewest tasks"""
        queue_vals = {qid: len(queues[qid][0]) for qid in self.qids}
        return min(queue_vals, key=queue_vals.get)
