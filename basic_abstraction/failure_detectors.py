from threading import Thread, Lock
import time

from base import Abstraction

start = time.time()

class PerfectFailureDetector(Abstraction):
    # Heartbeats
    REQUEST = 0
    REPLY = 1
    
    def __init__(self, link, abstraction_callback, operation_id_callback):
        super().__init__()
        self.abstraction_callback = abstraction_callback
        self.operation_id_callback = operation_id_callback
        self.event_handler_map = {
            PerfectFailureDetector.REQUEST: self.request,
            PerfectFailureDetector.REPLY: self.reply
        }

        self.peers = set()
        self.detected = set()
        self.correct = set()
        self.process_number = link.process_number

        self.send = link.register(self)
        self.lock = Lock()
        self.worker = Thread(target=self.detect_failures)

    def start(self):
        super().start()
        self.worker.start()

    def add_peers(self, *peers):
        self.peers.update(peers)

    def request(self, source_number, message):
        if self.debug:
            print(f"PFD: {self.process_number}: request from {source_number}")
        self.send(source_number, (self.REPLY, None))

    def reply(self, source_number, message):
        if self.debug:
            print(f"PFD: {self.process_number}: reply from {source_number}")
        with self.lock:
            self.correct.add(source_number)

    def detect_failures(self):
        while self.alive:
            self.send_heartbeats()
            time.sleep(self.TIMEOUT/10)
            self.timeout()

    def send_heartbeats(self):
        for peer in self.peers - self.detected:
            self.send(peer, (self.REQUEST, None))

    def timeout(self):
        with self.lock:
            for peer in self.peers - self.correct - self.detected:
                self.detected.add(peer)
                self.abstraction_callback.trigger_event(self.operation_id_callback, args=(peer,))
                if self.debug:
                    print(f"PFD: {self.process_number}: peer {peer} crashed")
            self.correct.clear()

if __name__ == "__main__":
    from link import PerfectLink
    timescale = 0.1
    class Test(Abstraction):
        CRASHED = 0
        def __init__(self, process_number):
            super().__init__()
            self.event_handler_map = {self.CRASHED: self.crashed}
            self.link = PerfectLink(process_number)
            self.pfd = PerfectFailureDetector(self.link, self, self.CRASHED)

        def start(self):
            super().start()
            self.link.start()
            self.pfd.start()

        def stop(self):
            super().stop()
            self.link.stop()
            self.pfd.stop()

        def crashed(self, peer):
            print(f"{self.link.process_number}: peer {peer} crashed")


    test0 = Test(0)
    test1 = Test(1)
    test2 = Test(2)

    test0.pfd.add_peers(1, 2)
    test1.pfd.add_peers(0, 2)
    test2.pfd.add_peers(0, 1)
    test0.start()
    """
    test1.abstr_worker.start()
    test1.pfd.abstr_worker.start()
    test1.link.start()
    test2.abstr_worker.start()
    test2.pfd.abstr_worker.start()
    test2.link.start()
    """
    test1.start()
    test2.start()
        
    time.sleep(15*timescale)
    test1.stop()
    time.sleep(15*timescale)
    test0.stop()
    time.sleep(15*timescale)
    test2.stop()
