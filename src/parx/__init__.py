"""parx"""
from typing import Callable, Iterable
from multiprocessing import current_process
from os import cpu_count

from tqdm import tqdm

from .multiprocess import run


__all__ = ["parx"]


def run_in_main_only(func: Callable):
    """run_in_main_only acts as an entry guard

    If `parx()` is not guarded by __name__=="__main__", spawned child processes
    will import and hence run everything again. `noop` does nothing. But if user
    attempts to print(parx(func, data)), it displays some information instead of
    simply None.

    Args:
        func (Callable): function
    """

    def noop(*args, **kwargs):  # pylint: disable=unused-argument
        return f"{current_process().name} running"

    if current_process().name == "MainProcess":
        return func
    return noop


@run_in_main_only
def parx(
    func: Callable, data: Iterable, progress_bar=True, workers=cpu_count()
) -> list:
    """parx executes the function on the data in a parallel way

    Args:
        func (Callable): function
        data (Iterable): data
        progress_bar (bool, optional): show progress. Defaults to True.
        workers (_type_, optional): number of workers. Defaults to cpu_count().

    Returns:
        list: list of results
    """
    assert isinstance(func, Callable)
    assert isinstance(data, Iterable)
    assert isinstance(progress_bar, bool)
    assert isinstance(workers, int)
    parallel = not (workers is None or workers <= 1)

    match (parallel, progress_bar):
        case False, False:
            print("Sequential execution with 1 worker")
            return list(map(func, data))
        case False, True:
            print("Sequential execution with 1 worker")
            return list(map(func, tqdm(data)))
        case True, True:
            print(f"Parallel execution with {workers} workers")
            with tqdm(total=len(data)) as pbar:
                return run(func, data, workers, progress_update=pbar.update)
