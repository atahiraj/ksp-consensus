from threading import Thread, Lock
import time

from basic_abstraction.base import Abstraction
from utils import Logging

class PerfectFailureDetector(Abstraction):
    # Heartbeats
    REQUEST = 0
    REPLY = 1
    
    def __init__(self, link):
        super().__init__()
        self.event_handler_map = {
            PerfectFailureDetector.REQUEST: self.request,
            PerfectFailureDetector.REPLY: self.reply
        }
        self.clients = []
        
        self.link = link
        self.process_number = self.link.process_number
        self.send = self.link.register(self)

        self.peers = set()
        self.detected = set()
        self.correct = set()

        self.lock = Lock()
        self.worker = Thread(target=self.detect_failures)
        self.logger = Logging(self.process_number, "PFD")

    def start(self):
        super().start()
        self.worker.start()

    def add_peers(self, *peers):
        self.peers.update(peers)

    def register(self, abstraction, operation_id):
        self.clients.append((abstraction, operation_id))

    def request(self, source_number):
        self.logger.log_debug(f"Request from {source_number}")
        self.send(source_number, self.REPLY)

    def reply(self, source_number):
        self.logger.log_debug(f"Reply from {source_number}")
        with self.lock:
            self.correct.add(source_number)

    def detect_failures(self):
        while self.alive:
            self.send_heartbeats()
            time.sleep(self.TIMEOUT/10)
            self.timeout()

    def send_heartbeats(self):
        for peer in self.peers - self.detected:
            self.send(peer, self.REQUEST)

    def timeout(self):
        with self.lock:
            for peer in self.peers - self.correct - self.detected:
                self.detected.add(peer)

                self.logger.log_debug(f"Peer {peer} crashed")
                for (abstraction, operation_id) in self.clients:
                    abstraction.trigger_event(operation_id, args=(peer,))

            self.correct.clear()

if __name__ == "__main__":
    from basic_abstraction.link import PerfectLink
    timescale = 0.1
    class Test(Abstraction):
        CRASHED = 0
        def __init__(self, process_number):
            super().__init__()
            self.event_handler_map = {self.CRASHED: self.crashed}
            self.link = PerfectLink(process_number)
            self.process_number = self.link.process_number
            self.pfd = PerfectFailureDetector(self.link)
            self.pfd.register(self, self.CRASHED)
            #Logging.set_debug(self.process_number, "PFD", True)

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
    test1.start()
    test2.start()
    
    time.sleep(15*timescale)
    test1.stop()
    time.sleep(15*timescale)
    test0.stop()
    time.sleep(15*timescale)
    test2.stop()
