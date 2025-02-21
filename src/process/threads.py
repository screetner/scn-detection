import threading

class ThreadWithException(threading.Thread):
    def __init__(self, target, args=(), kwargs=None):
        super().__init__()
        self._target_fn = target
        self._args = args
        self._kwargs = kwargs or {}
        self.exc = None

    def run(self):
        try:
            if self._target_fn:
                self._target_fn(*self._args, **self._kwargs)
        except Exception as e:
            self.exc = e

    def join(self, timeout=None):
        super().join(timeout)
        if self.exc:
            raise self.exc

def safe_thread(target, *args):
    try:
        target(*args)
    except Exception as e:
        print(f"Error in {target.__name__}: {e}")
        raise
