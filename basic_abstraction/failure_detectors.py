import threading
import time

class PerfectFailureDetector:
    def __init__(self, link, crash_callback):
        self.peers = []
        self.detected = []
        self.alive = []
        self.crash_callback = crash_callback
        self.link = link
        self.link.add_callback(self.receive)
        self.timeout_thread = None

    def start_heartbeat(self):
        self.timeout_thread = PerfectFailureDetector.TimeoutThread(self.timeout)
        self.timeout_thread.start()

    def add_peers(self, peers):
        self.peers.extend(peers)
        self.alive.extend(peers)

    def receive(self, source_number, message):
        if message == "heartbeat request".encode("utf-8"):
            self.link.send(source_number, "heartbeat reply".encode("utf-8"))
        elif message == "heartbeat reply".encode("utf-8") and source_number not in self.alive:
            self.alive.append(source_number)

    def timeout(self):
        old_alive = self.alive
        self.alive = []
        for peer in self.peers:
            if peer not in old_alive and peer not in self.detected:
                self.detected.append(peer)
                self.crash_callback(peer)
            self.link.send(peer, "heartbeat request".encode("utf-8"))

    class TimeoutThread(threading.Thread):
        def __init__(self, callback):
            super().__init__()
            self.callback = callback

        def run(self):
            while True:
                time.sleep(0.1)
                self.callback()

if __name__ == "__main__":
    from basic_abstraction.link import FairLossLink

    class Test:
        def __init__(self, process_number):
            self.link = FairLossLink(process_number)
            self.pfd = PerfectFailureDetector(self.link, self.callback)

        def callback(self, process_number):
            print("{}: Process {} has crashed".format(self.link.process_number, process_number))

    test0 = Test(0)
    test0.pfd.add_peers([1, 2])
    test1 = Test(1)
    test1.pfd.add_peers([0, 2])
    test2 = Test(2)
    test2.pfd.add_peers([1, 0])

    test0.pfd.start_heartbeat()
    test1.pfd.start_heartbeat()
    test2.pfd.start_heartbeat()

    time.sleep(1)

    test3 = Test(3)
    test3.pfd.add_peers([1, 2, 0])
    test3.pfd.start_heartbeat()
    test0.pfd.add_peers([3])
    test1.pfd.add_peers([3])
    test2.pfd.add_peers([3])

    time.sleep(1)
    test2.link.alive = False
    time.sleep(1)
    test1.link.alive = False
