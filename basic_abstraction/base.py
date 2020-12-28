from threading import Thread
from queue import Queue, Empty

from utils import Logging


class Abstraction:
    """This class implements an abstracton.

    An abstraction consists in an event queue and a thread that executes these
    events sequentially. Events are added through trigger_event method. Events
    correspond to one of the Abstraction's methods. For instance, a hierchical
    consensus object would have a "propose" method, one would then trigger the
    "propose" event to trigger the consensus. Events are triggered through the
    method itself or its name (see below.)

    """

    TIMEOUT = 1

    def __init__(self):
        self.alive = True
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
        event_name = self.stringify_event(event)
        self.event_queue.put((event_name, args, kwargs))

    def stringify_event(self, event):
        if isinstance(event, str):
            return event
        elif hasattr(event, "__name__"):
            return event.__name__
        else:
            raise Exception("Events must be strings or have __name__ attr")


class Subscribable(Abstraction):
    """This class implements a subscriptable abstraction.

    Subscribable abstractions are abstractions that remember multiple callbacks
    and may call them all. This class was made a bit too generic as it supports
    function callbacks although only triggering events is required in this
    project.

    Useful class for, say, a perfect failure detector. Multiple abstractions
    would subscribe to a perfect failure detector by providing an event which is
    to be triggered in case of peer failure.

    """

    def __init__(self):
        super().__init__()
        self.callbacks = []

    def call_callbacks(self, *args, **kwargs):
        for callback in self.callbacks:
            callback(*args, **kwargs)

    def subscribe(self, callback):
        self.callbacks.append(callback)

    def subscribe_abstraction(self, abstraction, event):
        event_name = self.stringify_event(event)

        def callback(*args, **kwargs):
            abstraction.trigger_event(event_name, args=args, kwargs=kwargs)

        self.subscribe(callback)


class Registrable(Abstraction):
    """This class implements a registrable abstraction.

    Registrable abstrations are able to remember a list of clients and redirect
    queries towards the said clients. This class was made a bit too generic as
    it is meant to support non Abstraction clients.

    The idea is to have registering methods returning wrappers around the
    trigger_event method. The wrappers would then add the client_it to the
    queries which are later used by this class to redirect the query to the
    correct client.

    This class was degined for links and broadcasts. Multiple abstractions would
    need to use a link abstraction to communicate, but multiple link
    abstractions might mean multiple unix sockets (see ./link.py). This class
    helps multiple abstractions to communicate through the same link. For
    instance, a perfect failure detector might register to a link to communicate
    with the perfect failure detector of other peers. Using the wrapper
    functions, the destination link can redirect a request from a remote perfect
    link to the corresponding local perfect link (since peers are identical,
    they would ave the same client id).

    One has to implement generate_caller and generate_abstraction_caller for
    functions and abstraction callbacks respectively.

    """

    def __init__(self):
        super().__init__()
        self.callbacks = []
        self.callback_id = 0

    def _register(self, callback):
        self.callbacks.append(callback)
        self.callback_id += 1

    def register(self, callback):
        self._register(callback)
        return self.generate_caller(self.callback_id - 1)

    def register_abstraction(self, abstraction):
        def callback(event, *args, **kwargs):
            event_name = self.stringify_event(event)
            abstraction.trigger_event(event_name, args=args, kwargs=kwargs)

        self._register(callback)
        return self.generate_abstraction_caller(self.callback_id - 1)

    def generate_caller(self, callback_id):
        pass

    def generate_abstraction_caller(self, callback_id):
        pass

    def callback(self, callback_id, args=(), kwargs={}):
        self.callbacks[callback_id](*args, **kwargs)


if __name__ == "__main__":
    import time

    class TestAbstr(Abstraction):
        def task0(self, string):
            time.sleep(0.5)
            print(f"task0: {string}")

        def task1(self, string):
            time.sleep(0.5)
            print(f"task1: {string}")

        def task2(self, string):
            time.sleep(0.5)
            print(f"task2: {string}")

    test = TestAbstr()
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

    # Testing subscriptable
    class TestSubscr(Abstraction):
        def __init__(self, number):
            super().__init__()
            self.number = number

        def print_stuff(self):
            print(f"{self.number}: Hello!")

    def callback():
        print("Function: Hello!")

    sub = Subscribable()
    test0 = TestSubscr(0)
    test1 = TestSubscr(1)
    test2 = TestSubscr(2)

    sub.subscribe_abstraction(test0, "print_stuff")
    sub.subscribe_abstraction(test1, TestSubscr.print_stuff)
    sub.subscribe_abstraction(test2, test2.print_stuff)
    sub.subscribe(callback)

    sub.start()
    test0.start()
    test1.start()
    test2.start()

    sub.trigger_event(sub.call_callbacks)

    time.sleep(0.5)
    sub.stop()
    test0.stop()
    test1.stop()
    test2.stop()

    # Testing Registrable
    class TestRegistr(Registrable):
        def entrypoint(self, callback_id, args, kwargs):
            self.callback(callback_id, args, kwargs)

        def generate_abstraction_caller(self, callback_id):
            def caller(event, args=(), kwargs={}):
                event_name = self.stringify_event(event)
                args = (event_name, *args)
                self.trigger_event(self.entrypoint, args=(callback_id, args, kwargs))

            return caller

    reg = TestRegistr()
    test0 = TestAbstr()
    reg.start()
    test0.start()
    caller = reg.register_abstraction(test0)

    caller(event="task0", args=("Hellooooo0",))
    caller(test0.task1, args=("Hellooooo1",))
    caller(event=TestAbstr.task2, kwargs={"string": "Hellooooo2"})

    reg.event_queue.join()
    test0.event_queue.join()
    reg.stop()
    test0.stop()
