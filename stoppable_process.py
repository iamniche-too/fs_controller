import multiprocessing


class StoppableProcess(multiprocessing.Process):
    """Process class with a stop() method. The process itself has to check
    regularly for the stopped() condition."""

    def __init__(self, *args, **kwargs):
        multiprocessing.Process.__init__(self)
        self._stop_event = multiprocessing.Event()

    def stop(self):
        print("StoppableProcess - stop called...")
        self._stop_event.set()

    def is_stopped(self):
        return self._stop_event.is_set()
