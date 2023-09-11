class InvalidStep(Exception):
    """Exception raised when the step() function is called when the game is done"""

    pass


class InvalidReset(Exception):
    """Exception raised when the reset() function is called when the state data has not been loaded"""

    pass


class EmptyInit(Exception):
    """Exception raised when the init_random_positions() func pics a ticker with no state data"""

    pass
