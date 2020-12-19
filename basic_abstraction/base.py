from threading import Thread
from queue import Queue, Empty

from utils import Logging

class Abstraction:
    TIMEOUT = 1
    def __init__(self):
        self.alive = True
        self.debug = False
        self.event_queue = Queue()
        self.event_handler_map = None
        self.base_worker = Thread(target=self.run_tasks)

    def start(self):
        self.base_worker.start()

    def stop(self):
        self.alive = False

    def run_tasks(self):
        while self.alive:
            try:
                event_flag, args, kwargs = self.event_queue.get(timeout=self.TIMEOUT)
            except Empty:
                continue
            else:
                if self.alive:
                    self.event_handler_map[event_flag](*args, **kwargs)
                self.event_queue.task_done()

    def trigger_event(self, event_flag, args=[], kwargs={}):
        self.event_queue.put((event_flag, args, kwargs))

if __name__ == "__main__":
    import time
    def task0(string):
        time.sleep(0.5)
        print(f"task0: {string}")

    def task1(string):
        time.sleep(0.5)
        print(f"task1: {string}")

    def task2(string):
        time.sleep(0.5)
        print(f"task2: {string}")

    test = Abstraction()
    test.start()
    test.event_handler_map = [task0, task1, task2]
    print("Adding hello")
    test.trigger_event(0, args=["hello0"])
    print("Adding hello2")
    test.trigger_event(1, args=["hello2"])
    print("Adding hello3")
    test.trigger_event(2, args=["hello3"])
    test.event_queue.join()

    # Testing if it stops correctly
    test.alive = False
    test.trigger_event(0, args=["helloX"])
    test.event_queue.join()
