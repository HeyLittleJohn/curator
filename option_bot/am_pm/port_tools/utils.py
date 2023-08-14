import numpy as np


def index_filler(array: np.ndarray, shape: tuple, fill_value: float | int, position: str = "start") -> np.ndarray:
    """Fills a given array with a given value so that it matches the desired shape.

    Args:
        position: str (default="end") can be "start" or "end" to indicate where to fill the array
    """
    if position == "start":
        return np.pad(array, (shape[0] - len(array), 0), "constant", constant_values=fill_value)
    elif position == "end":
        return np.pad(array, (0, shape[0] - len(array)), "constant", constant_values=fill_value)
