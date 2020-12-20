from threading import Thread
from queue import Queue, Empty

from utils import Logging

class Abstraction:
    TIMEOUT = 1
    def __init__(self):
        self.alive = True
        self.debug = False
        self.event_queue = Queue()
        self.base_worker = Thread(target=self.run_tasks)

    def start(self):
        self.base_worker.start()

    def stop(self):
        self.alive = False

    def run_tasks(self):
        while self.alive:
            try:
                event_name, args, kwargs = self.event_queue.get(timeout=self.TIMEOUT)
            except Empty:
                continue
            else:
                if self.alive:
                    getattr(self, event_name)(*args, **kwargs)
                self.event_queue.task_done()

    def trigger_event(self, event, args=(), kwargs={}):
        event_name = self.sanitize_event(event)
        self.event_queue.put((event_name, args, kwargs))

    def sanitize_event(self, event):
        if hasattr(event, "__name__"):
            return event.__name__
        elif isinstance(event, str):
            return event
        else:
            raise Exception("Events must be strings or have __name__ attr")

if __name__ == "__main__":
    import time
    class Test(Abstraction):
        def task0(self, string):
            time.sleep(0.5)
            print(f"task0: {string}")

        def task1(self, string):
            time.sleep(0.5)
            print(f"task1: {string}")

        def task2(self, string):
            time.sleep(0.5)
            print(f"task2: {string}")

    test = Test()
    test.start()
    print("Adding hello")
    test.trigger_event(test.task0, args=["hello0"])
    print("Adding hello2")
    test.trigger_event(test.task1, args=["hello2"])
    print("Adding hello3")
    test.trigger_event(test.task2, args=["hello3"])
    test.event_queue.join()

    # Testing if it stops correctly
    test.alive = False
    test.trigger_event(test.task0, args=["helloX"])
    test.event_queue.join()
