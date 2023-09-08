from threading import Lock


class ThreadSafeRemoteGraphConstruction:
    def __init__(self):
        self.lock = Lock()
